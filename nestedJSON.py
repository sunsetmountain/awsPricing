import csv
import requests

def safely_deep_get(dictionary, keys, default):
    node = dictionary
    for key in keys.split("."):
        if isinstance(node, dict):
            node = node.get(key, None)
        elif isinstance(node, (list, tuple)) and key.isnumeric():
            key = int(key)
            node = node[key] if len(node) > key else None
        elif hasattr(node, key):
            node = getattr(node, key)
        else:
            return default

    return node if node is not None else default


# # Load the json data
url = 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/index.json'

# Send a GET request to the URL
response = requests.get(url)

# Parse the json data
json_data = response.json()

# Open a csv file for writing
csv_file = open("output.csv", "w")
csv_writer = csv.writer(csv_file)

 # Write some meta information to the file
csv_writer.writerow(['FormatVersion', json_data.get('formatVersion')])
csv_writer.writerow(['Disclaimer', json_data.get('disclaimer')])
csv_writer.writerow(['Publication Date', json_data.get('publicationDate')])
csv_writer.writerow(['Version', json_data.get('version')])
csv_writer.writerow(['OfferCode', json_data.get('offerCode')])

# Write the header row
csv_writer.writerow(
    [
        "SKU",
        "OfferTermCode",
        "RateCode",
        "TermType",
        "PriceDescription",
        "EffectiveDate",
        "StartingRange",
        "EndingRange",
        "Unit",
        "PricePerUnit",
        "Currency",
        "Product Family",
        "serviceCode",
        "Location",
        "Location Type",
        "Availability",
        "Storage Class",
        "Volume Type",
        "Fee Code",
        "Fee Description",
        "Group",
        "Group Description",
        "Transfer Type",
        "From Location",
        "From Location Type",
        "To Location",
        "To Location Type",
        "usageType",
        "operation",
        "Durability",
        "From Region Code",
        "Overhead",
        "Region Code",
        "serviceName",
        "To Region Code",
    ]
)

# Iterate through the products
for sku, product in json_data["products"].items():
    # Get the corresponding term data
    term = json_data["terms"]["OnDemand"][sku][sku + ".JRTCKXETXF"]
    try:
        offer_term_code = safely_deep_get(term, "offerTermCode", "")
        term_type = "OnDemand"
        effective_date = safely_deep_get(term, "effectiveDate", "")
        product_family = safely_deep_get(product, "productFamily", "")
        service_code = safely_deep_get(product, "attributes.servicecode", "")
        location = safely_deep_get(product, "attributes.location", "")
        location_type = safely_deep_get(product, "attributes.locationType", "")
        availability = safely_deep_get(product, "attributes.availability", "")
        storage_class = safely_deep_get(product, "attributes.storageClass", "")
        volume_type = safely_deep_get(product, "attributes.volumeType", "")
        fee_code = safely_deep_get(product, "attributes.feeCode", "")
        fee_description = safely_deep_get(product, "attributes.feeDescription", "")
        group = safely_deep_get(product, "attributes.group", "")
        group_description = safely_deep_get(product, "attributes.groupDescription", "")
        transfer_type = safely_deep_get(product, "attributes.transferType", "")
        from_location_type = safely_deep_get(product, "attributes.fromLocationType", "")
        from_location = safely_deep_get(product, "attributes.fromLocation", "")
        to_location = safely_deep_get(product, "attributes.toLocation", "")
        to_location_type = safely_deep_get(product, "attributes.toLocationType", "")
        usage_type = safely_deep_get(product, "attributes.usagetype", "")
        operation = safely_deep_get(product, "attributes.operation", "")
        durability = safely_deep_get(product, "attributes.durability", "")
        from_region_code = safely_deep_get(product, "attributes.fromRegionCode", "")
        overhead = safely_deep_get(product, "attributes.overhead", "")
        region_code = safely_deep_get(product, "attributes.regionCode", "")
        servicename = safely_deep_get(product, "attributes.servicename", "")
        to_region_code = safely_deep_get(product, "attributes.toRegionCode", "")
        for i, (key, value) in enumerate(term["priceDimensions"].items()):
            rate_code = value.get('rateCode')
            price_description = value.get('description')
            begin_range = value.get('beginRange')
            end_range = value.get('endRange')
            unit = value.get('unit')
            price_per_unit = list(value['pricePerUnit'].values())[0]
            currency = list(value['pricePerUnit'].keys())[0]

            # Write the data to the csv
            csv_writer.writerow(
                [
                    sku,
                    offer_term_code,
                    rate_code,
                    term_type,
                    price_description,
                    effective_date,
                    begin_range,
                    end_range,
                    unit,
                    price_per_unit,
                    currency,
                    product_family,
                    service_code,
                    location,
                    location_type,
                    availability,
                    storage_class,
                    volume_type,
                    fee_code,
                    fee_description,
                    group,
                    group_description,
                    transfer_type,
                    from_location_type,
                    from_location,
                    to_location,
                    to_location_type,
                    usage_type,
                    operation,
                    durability,
                    from_region_code,
                    overhead,
                    region_code,
                    servicename,
                    to_region_code,
                ]
            )
    except Exception as e:
        print(e)
        print(term)

print("Nested JSON to Flat CSV Generated.....")
