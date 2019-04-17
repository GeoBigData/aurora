import boto3

service_code = 'Amazons3'

pricing_region = 'us-east-1'
# connect to the pricing client
pricing = boto3.client('pricing', region_name=pricing_region)

print("Selected {} Attributes & Values".format(service_code))
print("================================")
response = pricing.describe_services(ServiceCode=service_code)
attrs = response['Services'][0]['AttributeNames']

for attr in attrs:
    response = pricing.get_attribute_values(ServiceCode=service_code,
                                            AttributeName=attr)

    values = []
    for attr_value in response['AttributeValues']:
        values.append(attr_value['Value'])

    print("  " + attr + ": " + ", ".join(values))