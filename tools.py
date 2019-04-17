import boto3
import json
from pkg_resources import resource_filename

# Search product filter
FLT = '[{{"Field": "tenancy", "Value": "shared", "Type": "TERM_MATCH"}},'\
      '{{"Field": "operatingSystem", "Value": "{o}", "Type": "TERM_MATCH"}},'\
      '{{"Field": "preInstalledSw", "Value": "NA", "Type": "TERM_MATCH"}},'\
      '{{"Field": "instanceType", "Value": "{t}", "Type": "TERM_MATCH"}},'\
      '{{"Field": "location", "Value": "{r}", "Type": "TERM_MATCH"}}]'


def get_price_dims(client, region_description, instance_type, platform):
    f = FLT.format(r=region_description,
                   t=instance_type,
                   o=platform)
    data = client.get_products(ServiceCode='AmazonEC2', Filters=json.loads(f))
    od = json.loads(data['PriceList'][0])['terms']['OnDemand']
    id1 = list(od)[0]
    id2 = list(od[id1]['priceDimensions'])[0]
    usd = od[id1]['priceDimensions'][id2]['pricePerUnit']['USD']
    if float(usd) == 0 and len(data['PriceList']) > 1:
        od = json.loads(data['PriceList'][1])['terms']['OnDemand']
        id1 = list(od)[0]
        id2 = list(od[id1]['priceDimensions'])[0]
        usd = od[id1]['priceDimensions'][id2]['pricePerUnit']['USD']


    return usd


# Get current AWS price for an on-demand instance
def get_price(region_name, instance_type, platform, region_description=None):
    if region_description is None:
        region_description = get_region_description(region_name)
    client = boto3.client('pricing', region_name='us-east-1')
    usd = get_price_dims(client=client,
                         region_description=region_description,
                         instance_type=instance_type,
                         platform=platform)

    return usd


def get_all_regions():

    endpoint_file = resource_filename('botocore', 'data/endpoints.json')
    with open(endpoint_file, 'r') as f:
        data = json.load(f)
    regions = list(data['partitions'][0]['regions'].keys())

    return regions


# Translate region code to region name
def get_region_description(region_name):
    if len(region_name.split('-')[2]) > 1:
        # reformat to drop the last letter
        region_name = region_name[:-1]
    endpoint_file = resource_filename('botocore', 'data/endpoints.json')
    with open(endpoint_file, 'r') as f:
        data = json.load(f)
    return data['partitions'][0]['regions'][region_name]['description']



