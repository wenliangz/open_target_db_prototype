from pymongo import MongoClient
import pandas as pd
import json
from pprint import pprint


def mongoimport(client, csv_path, db_name, coll_name):
    """ Imports a csv file at path csv_name to a mongo colection
    returns: count of the documants in the new collection
    """
    db = client[db_name]
    coll = db[coll_name]
    data = pd.read_csv(csv_path)
    payload = json.loads(data.to_json(orient='records'))
    coll.delete_many({})
    coll.insert_many(payload)
    return coll.count_documents({})

# # Create database and three collections
client = MongoClient("mongodb://localhost:27017/")
db = client["opentarget"]
study=db.study
variant=db.variant
credset=db.credset
db.list_collection_names()

mongoimport(client, 'data/study_sample.csv', 'opentarget','study')
mongoimport(client, 'data/credset_sample.csv', 'opentarget','credset')
mongoimport(client, 'data/variant_sample_withkey.csv', 'opentarget','variant')

# create index to improve join performance
credset.create_index('study_id')
credset.create_index('tag_variant_id')

# # Explore the data in MongoDB using pyMongo
pprint(study.find_one())