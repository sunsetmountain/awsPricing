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
    # Set the file location and filename to save files
    filelocation = str(Path(__file__).parent.parent) + "/awsPricing/products/"
    base_filename = 'awsProducts-'
    offeringFilename = base_filename + offering
    termsFilename = base_filename + "Terms" + offering

    print(f"{offering}:{url}")
    response = requests.get(url)

    # Create arrays to store products and terms
    productitems = []
    termitems = {}

    productsJSON = response.json()['products']
    for i in productsJSON:
        productitems.append(productsJSON[i])

    termsJSON = response.json()['terms']
    # using keys example - https://stackoverflow.com/questions/34327719/get-keys-from-json-in-python#34327880
    terms = termsJSON.keys()
    for term in terms:
        #termitems.append('term':term)
        products = termsJSON[term].keys()
        for product in products:
            offerTerms = termsJSON[term][product].keys()
            for offerTerm in offerTerms:
                pricedimensions = termsJSON[term][product][offerTerm].keys()
                if type(termsJSON[term][product][offerTerm]) == dict:
                    print(f"OfferTerm Dict found: {(termsJSON[term][product][offerTerm])}")
                else:
                    print(f"OfferTerm Key pair found: {(termsJSON[term][product][offerTerm])}")
                for pricedimension in pricedimensions:
                    if type(termsJSON[term][product][offerTerm][pricedimension]) == dict:
                        print(f"PriceDimension Dict found: {(termsJSON[term][product][offerTerm][pricedimension])}")
                    else:
                        print(f"PriceDimension Key pair found: {(termsJSON[term][product][offerTerm][pricedimension])}")
                    #print(product)
                    #print(termsJSON[term][product])
                    #termitems.append(termsJSON[term][product][pricedimension])

    #flatten out the json
    #dfProducts = pd.json_normalize(productitems)
    dfTerms = pd.json_normalize(termitems)

    print(dfTerms)

    #shorten the header names
    #for col in dfProducts.columns:
    #    colName = col
    #   dotPosition = colName.rfind(".")
    #   if dotPosition > -1:
    #       newName = colName[dotPosition+1:]
    #       dfProducts.rename({colName: newName}, axis=1, inplace=True)

    ## Save the data frame as a CSV file
    ##dfProducts.to_csv(os.path.join(filelocation,offeringFilename) + '.csv', index=False)
    dfTerms.to_csv(os.path.join(filelocation,termsFilename) + '.csv', index=False)

if __name__ == "__main__":
    main()
