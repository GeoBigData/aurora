import boto3
import pandas as pd
import tqdm
import tools
import time
import json
import click

ebs_name_map = {
    'standard': 'Magnetic',
    'gp2': 'General Purpose',
    'io1': 'Provisioned IOPS',
    'st1': 'Throughput Optimized HDD',
    'sc1': 'Cold HDD'
}
tags_keys = ['Name', 'Team Owner', 'Team', 'Product Owner', 'Product', 'Creator']

@click.command()
@click.argument('in_region')
@click.argument('out_csv')
def main(in_region, out_csv):

    pricing_region = 'us-east-1'
    # connect to the pricing client
    pricing = boto3.client('pricing', region_name=pricing_region)
    ebs_price_lkup = {}
    results = []

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

    for region_name in region_names:
        # get the region description
        region_description = tools.get_region_description(region_name)
        print("Getting EBS Pricing for Region {desc} ({name})".format(desc=region_description,
                                                                      name=region_name))
        # initialize a dictionary to hold the pricing info for this region
        ebs_price_lkup[region_name] = dict()
        # get the pricing info
        for ebs_code in ebs_name_map:
            response = pricing.get_products(ServiceCode='AmazonEC2', Filters=[
                {'Type': 'TERM_MATCH', 'Field': 'volumeType', 'Value': ebs_name_map[ebs_code]},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': region_description}])
            for result in response['PriceList']:
                json_result = json.loads(result)
                for json_result_level_1 in json_result['terms']['OnDemand'].values():
                    for json_result_level_2 in json_result_level_1['priceDimensions'].values():
                        for price_value in json_result_level_2['pricePerUnit'].values():
                            continue
            ebs_price_lkup[region_name][ebs_code] = float(price_value)

        # get all of the volumes in this region
        print("Finding EBS Volumes in Region {desc} ({name})".format(desc=region_description,
                                                                     name=region_name))
        ec2 = boto3.resource('ec2', region_name=region_name)
        # list out all volumes in this region
        volumes = list(ec2.volumes.all())
        if len(volumes) > 0:
            for volume in tqdm.tqdm(volumes):
                # pause for a beat so print statements and tqdm don't get jumbled
                time.sleep(0.5)
                volume_info = dict()
                volume_info['id'] = volume.id
                volume_info['volume_type'] = volume.volume_type
                volume_info['size_gb'] = volume.size
                volume_info['region_name'] = region_name
                volume_info['region_desc'] = region_description
                volume_info['state'] = volume.state
                volume_info['usd_per_gb'] = ebs_price_lkup[region_name][volume_info['volume_type']]
                volume_info['usd_per_month'] = volume_info['usd_per_gb'] * volume_info['size_gb']
                # get associated ec2 info
                if len(volume.attachments) > 0:
                    volume_info['ec2_instance_id'] = volume.attachments[0]['InstanceId']
                    instance = ec2.Instance(id=volume_info['ec2_instance_id'])
                    if instance.tags is not None:
                        instance_tags = dict([x.values() for x in instance.tags if list(x.values())[0] in tags_keys])
                        volume_info.update(instance_tags)
                else:
                    volume_info['ec2_instance_id'] = 'NA'
                results.append(volume_info)
        else:
            print("No EBS Volumes found in this region.")

    results_df = pd.DataFrame(results)
    # results_df = results_df[:]
    results_df.sort_values(by='usd_per_month', inplace=True, ascending=False)
    results_df.to_csv(out_csv, index=False)

if __name__ == '__main__':
    main()






