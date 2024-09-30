import os, sys
from dotenv import load_dotenv, find_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk
from realestate_analytics.data.es import Datastore

def copy_mkt_trends_data(uat_datastore: Datastore, prod_datastore: Datastore):
  # Create the index in PROD
  prod_datastore.create_mkt_trends_index()

  # Fetch all documents from UAT
  uat_docs = list(scan(uat_datastore.es,
                       index=uat_datastore.mkt_trends_index_name,
                       query={"query": {"match_all": {}}}))

  # Prepare documents for bulk insertion
  actions = [
    {
      "_index": prod_datastore.mkt_trends_index_name,
      "_id": doc["_id"],
      "_source": doc["_source"]
    }
    for doc in uat_docs
  ]

  # Bulk insert into PROD
  success, failed = bulk(prod_datastore.es, actions, stats_only=True)

  print(f"Successfully copied {success} documents")
  if failed:
    print(f"Failed to copy {failed} documents")

  return success, failed

if __name__ == "__main__":
  _ = load_dotenv(find_dotenv())

  # UAT Datastore
  uat_host = os.getenv('UAT_ES_HOST')
  uat_port = int(os.getenv('UAT_ES_PORT'))
  uat_datastore = Datastore(host=uat_host, port=uat_port)

  # PROD Datastore
  prod_host = os.getenv('PROD_ES_HOST')
  prod_port = int(os.getenv('PROD_ES_PORT'))
  prod_datastore = Datastore(host=prod_host, port=prod_port)

  if uat_datastore.ping() and prod_datastore.ping():
    print("Connected to UAT and PROD datastores successfully")
    print(f'uat_host: {uat_host}, uat_port: {uat_port}')
    print(f'prod_host: {prod_host}, prod_port: {prod_port}')
  else:
    print("Failed to connect to UAT and PROD datastores")
    sys.exit(1)

  # Copy data
  success, failed = copy_mkt_trends_data(uat_datastore, prod_datastore)

  # Close connections
  uat_datastore.close()
  prod_datastore.close()