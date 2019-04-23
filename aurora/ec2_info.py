import boto3
import pandas as pd
import tqdm
import tools
import time
import click

keys = ['InstanceType',  'State', 'InstanceId', 'KeyName', 'LaunchTime', 'Placement', 'Platform','StateTransitionReason', 'SubnetId', 'VpcId', 'Architecture', 'Tags']
tags_keys = ['Name', 'Team Owner', 'Team', 'Product Owner', 'Product', 'Creator']
sorted_keys = tags_keys + ['usd_per_hr', 'usd_per_month'] + keys


@click.command()
@click.argument('in_region')
@click.argument('out_csv')
def main(in_region, out_csv):

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

    results = []
    for region_name in region_names:
        # get the region name (needed later for pricing)
        region_description = tools.get_region_description(region_name=region_name)
        print("Finding EC2s in Region {region_description} ({region_name})".format(region_description=region_description,
                                                                                   region_name=region_name))
        # create ec2 client for this region
        ec2 = boto3.client('ec2',
                           region_name=region_name)
        # get info on all of the EC2s
        instance_info = ec2.describe_instances()
        if len(instance_info['Reservations']) > 0:
            print("Getting EC2 Information")
            # pause for a beat so print statements and tqdm don't get jumbled
            time.sleep(0.5)
            # loop over EC2s and parse out the info needed
            instances = []
            for reservation_info in instance_info['Reservations']:
                instances.extend(reservation_info['Instances'])
            for instance in tqdm.tqdm(instances):
                instance_summary = {k: '' for k in keys}
                if 'Tags' in instance.keys():
                    tags = dict([x.values() for x in instance['Tags'] if list(x.values())[0] in tags_keys])
                else:
                    tags = dict()
                instance_summary.update(tags)
                for k in keys:
                    if k in instance.keys():
                        instance_summary[k] = instance[k]
                # get pricing info
                instance_type = instance_summary['InstanceType']
                if instance_summary['Platform'] == '':
                    instance_summary['Platform'] = 'Linux'
                platform = instance_summary['Platform']
                # Get current price for a given instance, region and os
                price = tools.get_price(region_name=region_name,
                                        instance_type=instance_type,
                                        platform=platform,
                                        region_description=region_description)
                instance_summary['usd_per_hr'] = float(price)
                instance_summary['usd_per_month'] = float(price) * 24 * 30
                results.append(instance_summary)
        else:
            print("No EC2s found in this region.")

    results_df = pd.DataFrame(results)
    results_df = results_df[sorted_keys]
    results_df.sort_values(by='usd_per_month', inplace=True)
    results_df.to_csv(out_csv, index=False)

if __name__ == '__main__':
    main()

