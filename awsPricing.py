import requests
import json
import pandas as pd
import os
from pathlib import Path
import time
import random

def main():
    # Use the AWS Price List Bulk API
    base_url = "https://pricing.us-east-1.amazonaws.com"
    offer_index_url = "/offers/v1.0/aws/index.json"
    request_url = base_url + offer_index_url
    offerURLs = readOfferIndexFile(request_url)
    number_of_offers = 0
    for i in offerURLs:
        number_of_offers += 1
        readOfferFile(i, base_url + offerURLs[i])
    print(f"Number of offerings: {number_of_offers}")

def readOfferIndexFile(url):
    response = requests.get(url)
    # Create a dict to store offer list
    offeritems= {}
    # Add the retail prices returned in the API response to a list
    for i in response.json()['offers']:
        offer_url = response.json()['offers'][i]['currentVersionUrl']
        offeritems[i]=offer_url
    return offeritems

def readOfferFile(offering, url):
    print(f"{offering}:{url}")
    response = requests.get(url)
    readProducts(offering, response)
    readTerms(offering, response)

def readProducts(offering, response):
    # Set the file location and filename to save files
    filelocation = str(Path(__file__).parent) + "/awsPricing/products/"
    filename = 'awsProducts-' + offering
    # Create arrays to store products and terms
    productitems = []
    productsJSON = response.json()['products']
    for i in productsJSON:
        productitems.append(get_simple_keys(productsJSON[i]))
    #flatten out the json
    dfProducts = pd.read_json(json.dumps(productitems))

    #shorten the header names
    for col in dfProducts.columns:
        colName = col
        dotPosition = colName.rfind(".")
        if dotPosition > -1:
            newName = colName[dotPosition+1:]
            dfProducts.rename({colName: newName}, axis=1, inplace=True)
    # Save the data frame as a CSV file
    dfProducts.to_csv(os.path.join(filelocation,filename) + '.csv', index=False)

def readTerms(offering, response):
    # Set the file location and filename to save files
    filelocation = str(Path(__file__).parent) + "/awsPricing/products/"
    filename = 'awsTerms-' + offering
    # Create arrays to store products and terms
    items = ""
    termsJSON = response.json()['terms']
    # print(termsJSON)
    # sys.exit()
    terms = termsJSON.keys()
    result = []
    for term in terms:
        products = termsJSON[term].keys()
        for product in products:
            productItems = get_simple_keys(termsJSON[term][product])
            productItems['terms'] = term
            result.append(productItems)
    jsonString = json.dumps(result)
    #read the JSON into Pandas
    dfTerms = pd.read_json(jsonString)
    # Save the data frame as a CSV file
    dfTerms.to_csv(os.path.join(filelocation,filename) + '.csv', index=False)

def get_simple_keys(data, result={}):
    #from https://stackoverflow.com/questions/34327719/get-keys-from-json-in-python#34327880
    for key in data.keys():
        if type(data[key]) not in [dict, list]:
            result[key] = data[key]
            print(result)
        elif type(data[key]) == list:
            continue
        else:
            #print(f"dict data type - key: {key} value: {data[key]}")
            get_simple_keys(data[key])
    return result

if __name__ == "__main__":
    main()
