import boto3
import pandas as pd
import tqdm
import tools
import json
import click
import datetime
import numpy as np
import botocore

@click.command()
@click.argument('in_region')
@click.argument('out_csv')
@click.option('--days', '-d', type=int, default=4)
def main(in_region, out_csv, days=4):

    pricing_region = 'us-east-1'
    # connect to the pricing client
    pricing = boto3.client('pricing', region_name=pricing_region)

    expected_region_names = tools.get_all_regions()
    if in_region == 'all':
        # process all regions
        region_names = expected_region_names
    else:
        # check that the specified region is valid
        if in_region not in expected_region_names:
            raise ValueError("in_region not recognized: {}".format(in_region))
        else:
            region_names = [in_region]

    s3_price_lkup = []
    for region_name in region_names:
        # get the region description
        region_description = tools.get_region_description(region_name)
        print("Getting S3 Pricing for Region {desc} ({name})".format(desc=region_description,
                                                                     name=region_name))

        region_description = tools.get_region_description(region_name)
        response = pricing.get_products(ServiceCode='AmazonS3', Filters=[
            {'Type': 'TERM_MATCH', 'Field': 'productFamily', 'Value': 'Storage'},
            {'Type': 'TERM_MATCH', 'Field': 'volumeType', 'Value': 'Standard'},  # assume standard volume
            {'Type': 'TERM_MATCH', 'Field': 'storageClass', 'Value': 'General Purpose'},
            # assume general purpose storage
            {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': region_description}])
        price_list = json.loads(response['PriceList'][0])
        price_dims = list(list(price_list['terms']['OnDemand'].values())[0]['priceDimensions'].values())
        for price_dim in price_dims:
            prices = {'region_name': region_name,
                      'min_bytes': float(price_dim['beginRange']),
                      'max_bytes': float(price_dim['endRange']),
                      'price_per_gb': float(price_dim['pricePerUnit']['USD'])
                      }
            s3_price_lkup.append(prices)

    s3_price_lkup_df = pd.DataFrame(s3_price_lkup)

    results = []
    client = boto3.client('s3')

    print("Finding all S3 Buckets.")
    buckets = client.list_buckets()['Buckets']
    if len(buckets) > 0:
        print("Calculating bucket storage and costs.")
        for bucket in tqdm.tqdm(buckets):
            bucket_name = bucket['Name']
            try:
                region_name = client.head_bucket(Bucket=bucket_name)['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']
            except botocore.exceptions.ClientError:
                try:
                    region_name = client.get_bucket_location(Bucket=bucket_name)['LocationConstraint']
                    if region_name is None:
                        region_name = 'us-east-1'
                except botocore.exceptions.ClientError:
                    msg = "Warning: Forbidden from determining region_name of {}. Assuming it is us-east-1".format(bucket_name)
                    print(msg)
            metric = {'Namespace': 'AWS/S3',
                       'MetricName': 'BucketSizeBytes',
                       'Dimensions': [{'Name': 'StorageType', 'Value': 'StandardStorage'},
                                      {'Name': 'BucketName', 'Value': bucket_name}]}

            metric_data_queries = [{'Id'        : 'a',
                                    'MetricStat': {'Metric': metric,
                                                   'Period': 3600,
                                                   'Stat'  : 'Average',
                                                   'Unit'  : 'Bytes'}
                                    }]
            cloudwatch = boto3.client('cloudwatch',
                                      region_name=region_name)
            metric_data = cloudwatch.get_metric_data(MetricDataQueries=metric_data_queries,
                                                     StartTime=datetime.datetime.now()-datetime.timedelta(days=days),
                                                     EndTime=datetime.datetime.now())

            if len(metric_data['MetricDataResults']) > 0 and len(metric_data['MetricDataResults'][0]['Values']) > 0:
                bucket_size_bytes = float(metric_data['MetricDataResults'][0]['Values'][0])
            else:
                bucket_size_bytes = np.nan

            bucket_size_gb = bucket_size_bytes / 1e9
            if not np.isnan(bucket_size_bytes):
                # look up the costs
                query_string = "region_name == '{region_name}' &  min_bytes <= {bucket_size_bytes} & max_bytes >= {bucket_size_bytes}".format(
                        bucket_size_bytes=bucket_size_bytes,
                        region_name=region_name
                )
                price_per_gb = s3_price_lkup_df.query(query_string)['price_per_gb'].tolist()[0]
            else:
                price_per_gb = np.nan

            usd_per_month = price_per_gb * bucket_size_gb
            results.append({'bucket_name': bucket_name,
                            'bucket_size_bytes': bucket_size_bytes,
                            'bucket_size_gb': bucket_size_gb,
                            'price_per_gb': price_per_gb,
                            'usd_per_month': usd_per_month,
                            'region_name': region_name})
    else:
        print("No S3 Buckets found.")

    results_df = pd.DataFrame(results)
    # results_df = results_df[:]
    results_df.sort_values(by='usd_per_month', inplace=True, ascending=False)
    results_df.to_csv(out_csv, index=False)

if __name__ == '__main__':
    main()



