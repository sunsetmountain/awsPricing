import requests
import json
import ijson
import pandas as pd
import os
from pathlib import Path
import time
from datetime import datetime, timedelta
import random

def main():
    startTime = datetime.today()
    print(f"Started at: {startTime}")

    # Use the AWS Price List Bulk API
    base_url = "https://pricing.us-east-1.amazonaws.com"
    offer_index_url = "/offers/v1.0/aws/index.json"
    request_url = base_url + offer_index_url
    offerURLs = getOfferIndex(request_url)

    number_of_offers = 0
    for i in offerURLs:
        number_of_offers += 1
        readOfferFile(i, base_url + offerURLs[i])

    print(f"Number of offerings: {number_of_offers}")

    mergeFiles()

    stopTime = datetime.today()

    print(f"Finished at: {stopTime}")
    print(f"Processing time: {stopTime - startTime}")


def getOfferIndex(url):
    response = requests.get(url)

    # Create a dict to store offer list
    offeritems= {}

    # Add the retail prices returned in the API response to a list
    for i in response.json()['offers']:
        offer_url = response.json()['offers'][i]['currentVersionUrl']
        offeritems[i]=offer_url

    saveOfferIndexFile(response, offeritems)

    return offeritems

def saveOfferIndexFile(response, offeritems):
    filelocation = str(Path(__file__).parent) + "/offerIndex/"
    filename = "awsOfferIndex"
    filepath = filelocation + filename + '.json'
    with open(filepath, "w") as f:
        f.write(response.text)

    filepath = filelocation + filename + '.csv'
    df = pd.DataFrame.from_dict(offeritems, orient='index')
    df.to_csv(filepath)


def readOfferFile(offering, url):
    print(f"{offering} @ {url}")
    response = requests.get(url, stream=True)

    jsonFilePath = saveJSONFile(offering, response)
    print(f"{offering} JSON saved at: {datetime.today()}")

    readProducts(offering, jsonFilePath)
    print(f"{offering} Product CSV saved at: {datetime.today()}")

    readTerms(offering, jsonFilePath)
    print(f"{offering} Terms CSV saved at: {datetime.today()}")

def saveJSONFile(offering, response):
    filelocation = str(Path(__file__).parent) + "/json/"
    filename = offering
    filepath = filelocation + filename + '.json'
    with open(filepath, "w") as f:
        f.write(response.text)
    return filepath

def readProducts(offering, fileinputpath):
    # Set the file location and filename to save files
    fileoutputlocation = str(Path(__file__).parent) + "/products/"
    fileoutputname = 'awsProducts-' + offering

    # Create arrays to store products and terms
    productitems = []

    with open(fileinputpath) as f:
        for productsJSON in ijson.items(f, 'products'):
            for i in productsJSON:
                #flatten out the JSON
                keys = {}
                keys = get_simple_keys(productsJSON[i])
                # Avoid getting duplicates - https://stackoverflow.com/questions/23724136/appending-a-dictionary-to-a-list-in-a-loop
                data = keys.copy()
                productitems.append(data)

    #f = open(fileinputpath)
    #data = json.load(f)
    #productsJSON = data['products']
    ##productsJSON = response.json()['products']
    #for i in productsJSON:
    #    #print(f"Products JSON: {productsJSON[i]}")
    #    #flatten out the JSON
    #    keys = get_simple_keys(productsJSON[i])
    #    # Avoid getting duplicates - https://stackoverflow.com/questions/23724136/appending-a-dictionary-to-a-list-in-a-loop
    #    data = keys.copy()
    #    productitems.append(data)
    #
    #f.close()

    #Read the JSON into a dataframe
    dfProducts = pd.read_json(json.dumps(productitems))

    #shorten the header names
    #for col in dfProducts.columns:
    #    colName = col
    #    dotPosition = colName.rfind(".")
    #    if dotPosition > -1:
    #        newName = colName[dotPosition+1:]
    #        dfProducts.rename({colName: newName}, axis=1, inplace=True)

    # Save the data frame as a CSV file
    dfProducts.to_csv(os.path.join(fileoutputlocation,fileoutputname) + '.csv', index=False)

def readTerms(offering, fileinputpath):
    # Set the file location and filename to save files
    filelocation = str(Path(__file__).parent) + "/terms/"
    filename = 'awsTerms-' + offering
    
    # Create list to store products and terms
    termsResults = []

    with open(fileinputpath) as f:
        for termJSON in ijson.items(f, 'terms'):
            for term in termJSON:
                for product in termJSON[term]:
                    productJSON = termJSON[term][product]   
                    #print(f"PRODUCT: {productJSON}")                
                    #flatten out the JSON
                    termKeys = get_term_keys(productJSON)
                    #print(f"TERM KEYS: {termKeys}")   
                    termKeys['terms'] = term
                    # Avoid getting duplicates - https://stackoverflow.com/questions/23724136/appending-a-dictionary-to-a-list-in-a-loop
                    termData = termKeys.copy()
                    #print(f"DATA: {termData}")
                    termsResults.append(termData)


    #f = open(fileinputpath)
    #data = json.load(f)
    #termsJSON = data['terms']
    ##termsJSON = response.json()['terms']
    #terms = termsJSON.keys()
    #
    #for term in terms:
    #    products = termsJSON[term].keys()
    #    for product in products:
    #        keys = get_simple_keys(termsJSON[term][product])
    #        keys['terms'] = term
    #        # Avoid getting duplicates - https://stackoverflow.com/questions/23724136/appending-a-dictionary-to-a-list-in-a-loop
    #        data = keys.copy()
    #        result.append(data)
    #
    #f.close()

    jsonString = json.dumps(termsResults)

    #read the JSON into Pandas
    dfTerms = pd.read_json(jsonString)

    # Save the data frame as a CSV file
    dfTerms.to_csv(os.path.join(filelocation,filename) + '.csv', index=False)

def get_simple_keys(data, result={}):
    #approach from https://stackoverflow.com/questions/34327719/get-keys-from-json-in-python#34327880
    #print(f"INPUT DATA: {data}")
    for key in data.keys():
        if type(data[key]) not in [dict, list]:
            result[key] = data[key]
        elif type(data[key]) == list:
            continue
        else:
            get_simple_keys(data[key])
    return result

def get_term_keys(data, result={}):
    #approach from https://stackoverflow.com/questions/34327719/get-keys-from-json-in-python#34327880
    #print(f"INPUT DATA: {data}")
    for key in data.keys():
        if type(data[key]) not in [dict, list]:
            result[key] = data[key]
        elif type(data[key]) == list:
            continue
        else:
            get_term_keys(data[key])
    return result

def mergeFiles():
    offerList = readOfferIndexFile()
    total_offers = len(offerList)
    print(total_offers)
    offer_count = 0
    merged_products_df = pd.DataFrame()
    merged_terms_df = pd.DataFrame()

    for i in offerList:
        offer_count += 1
        print(f"({offer_count}/{total_offers}) {i}")

        filepath = f"{str(Path(__file__).parent)}/products/awsProducts-{i}.csv"
        product_df = readFile(filepath)
        df_list = [merged_products_df, product_df]
        merged_products_df = pd.concat(df_list)
    
    writeMergedFile(merged_products_df, "awsProductsMerged")

    for i in offerList:
        offer_count += 1
        print(f"({offer_count}/{total_offers}) {i}")

        filepath = f"{str(Path(__file__).parent)}/terms/awsTerms-{i}.csv"
        terms_df = readFile(filepath)
        df_list = [merged_terms_df, terms_df]
        merged_terms_df = pd.concat(df_list)
    
    writeMergedFile(merged_terms_df, "awsTermsMerged")


    print(f"Number of Products: {len(merged_products_df.index)}")
    print(f"Number of Terms: {len(merged_terms_df.index)}")

def readOfferIndexFile():
    # Set the file location and filename to read files
    filelocation = str(Path(__file__).parent) + "/offerIndex/"
    filename = "awsOfferIndex"
    filepath = filelocation + filename + '.json'

    with open(filepath, 'r') as f:
        jsonInput = json.load(f)

    offers = jsonInput["offers"]
    offerCodes = []
    for i in offers:
        offerCodes.append(i)

    return offerCodes

def readFile(filepath):
    # Set the file location and filename to read files
    try:
        df = pd.read_csv(filepath)

        return df
    except:
        df = pd.DataFrame()
        return df
    
def writeMergedFile(dataframe, fileoutputname):
    # Set the file location and filename to read files
    fileoutputlocation = str(Path(__file__).parent)

    dataframe.to_csv(os.path.join(fileoutputlocation,fileoutputname) + '.csv', index=False)

if __name__ == "__main__":
    main()
