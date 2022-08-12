from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

client = MongoClient()
zillow_db = client.zillow
zillow_data_db = client.zillowdata
zillow_granular_db = client.zillowgranular
zillow_test_db = client.zillowtest

zillow_list = [zillow_data_db, zillow_db, zillow_granular_db, zillow_test_db]

final_db = client.zillow_final_test
final_collection = final_db.zillowCollection
main_key_set = set()
home_info = set()
changing_keys = set()
for db in zillow_list:
    collection = db.zillowCollection
    results = collection.find()
    for doc in results:
        current_doc = {
            '_id': doc['id'],
            'streetViewURL': None if 'streetViewURL' not in doc.keys() else doc['streetViewURL'],
            'streetViewMetadataURL': None if 'streetViewMetadataURL' not in doc.keys() else doc[
                'streetViewMetadataURL'],
            'imgSrc': None if 'imgSrc' not in doc.keys() else doc['imgSrc'],
            'lotArea': None if 'lotArea' not in doc.keys() else doc['lotArea'],
            'statusType': None if 'statusType' not in doc.keys() else doc['statusType'],
            'lotAreaString': None if 'lotAreaString' not in doc.keys() else doc['lotAreaString'],
            'hdpData': None if 'hdpData' not in doc.keys() else doc['hdpData'],
            'pricePerSqft': None if 'pricePerSqft' not in doc.keys() else doc['pricePerSqft'],
            'lotSize': None if 'lotSize' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo'][
                'lotSize'],
            'soldPrice': None if 'soldPrice' not in doc.keys() else doc['soldPrice'],
            'brokerPhone': None if 'brokerPhone' not in doc.keys() else doc['brokerPhone'],
            'latLong': None if 'latLong' not in doc.keys() else doc['latLong'],
            'brokerName': None if 'brokerName' not in doc.keys() else doc['brokerName'],
            'area': None if 'area' not in doc.keys() else doc['area'],
            'photoCount': None if 'photoCount' not in doc.keys() else doc['photoCount'],
            'addressWithZip': None if 'addressWithZip' not in doc.keys() else doc['addressWithZip'],
            'address': None if 'address' not in doc.keys() else doc['address'],
            'baths': None if 'baths' not in doc.keys() else doc['baths'],
            'detailUrl': None if 'detailUrl' not in doc.keys() else doc['detailUrl'],
            'sgapt': None if 'sgapt' not in doc.keys() else doc['sgapt'],
            'festimate': None if 'festimate' not in doc.keys() else doc['festimate'],
            'zestimate': None if 'zestimate' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['zestimate'],
            'city': None if 'city' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['city'],
            'priceChange': None if 'priceChange' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['priceChange'],
            'homeStatusForHDP': None if 'homeStatusForHDP' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo'][
                'homeStatusForHDP'],
            'comingSoonOnMarketDate': None if 'comingSoonOnMarketDate' not in doc.keys() else
            doc['hdpData']['homeInfo']['comingSoonOnMarketDate'],
            'priceReduction': None if 'priceReduction' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo'][
                'priceReduction'],
            'rentZestimate': None if 'rentZestimate' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['rentZestimate'],
            'isPreforeclosureAuction': None if 'isPreforeclosureAuction' not in doc.keys() else
            doc['hdpData']['homeInfo']['isPreforeclosureAuction'],
            'streetAddress': None if 'streetAddress' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['streetAddress'],
            'unit': None if 'unit' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['unit'],
            'rentalPetsFlags': None if 'rentalPetsFlags' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo'][
                'rentalPetsFlags'],
            'listing_sub_type': None if 'listing_sub_type' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo'][
                'listing_sub_type'],
            'bathrooms': None if 'bathrooms' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['bathrooms'],
            'datePriceChanged': None if 'datePriceChanged' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo'][
                'datePriceChanged'],
            'newConstructionType': None if 'newConstructionType' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo'][
                'newConstructionType'],
            'rentalRefreshTime': None if 'rentalRefreshTime' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo'][
                'rentalRefreshTime'],
            'group_type': None if 'group_type' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['group_type'],
            'homeType': None if 'homeType' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['homeType'],
            'price': doc['price'] if 'price' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['price'],
            'bedrooms': None if 'bedrooms' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['bedrooms'],
            'yearBuilt': None if 'yearBuilt' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['yearBuilt'],
            'zipcode': None if 'zipcode' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['zipcode'],
            'dateSold': None if 'dateSold' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['dateSold'],
            'homeStatus': None if 'homeStatus' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['homeStatus'],
            'streetAddressOnly': None if 'streetAddressOnly' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo'][
                'streetAddressOnly'],
            'taxAssessedValue': None if 'taxAssessedValue' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo'][
                'taxAssessedValue'],
            'state': None if 'state' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['state'],
            'priceForHDP': None if 'priceForHDP' not in doc['hdpData']['homeInfo'].keys() else doc['hdpData']['homeInfo']['priceForHDP'],
        }
        try:
            final_collection.insert_one(current_doc)
        except DuplicateKeyError:
            continue



