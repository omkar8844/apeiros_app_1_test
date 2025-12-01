import streamlit as st
import pandas as pd
import json
from typing import Optional, List, Dict

from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError
from bson import ObjectId

MONGO_URI = st.secrets["mongodb"]["uri"]

@st.cache_resource
def get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

def test_connection(client: MongoClient):
    try:
        client.admin.command("ping")
        return True
    except ServerSelectionTimeoutError:
        return False


client = get_mongo_client()

if test_connection(client):
    st.success("Connected to MongoDB successfully!")
else:
    st.error("Failed to connect to MongoDB.")


if test_connection(client):
    st.success("Connected to MongoDB successfully!")

    db_retail = client["apeirosretail"]
    store_details = db_retail["storeDetails"]

    db_bills = client["apeirosretaildataprocessing"]
    bill_requests = db_bills["billRequest"]

    st.write("Databases ready to use!")
else:
    st.error("Could not connect to database.")

st.title("Displaying a Pandas DataFrame in Streamlit")
store_df=pd.DataFrame(list(store_details.find()))
store_df=store_df.rename(columns={'_id':'storeId'})
# bill_df=pd.DataFrame(list(bill_requests.find())) 
# bill_df=bill_df.merge(store_df[['storeId','storeName']],on='storeId',how='inner')
# st.dataframe(bill_df)
# Fetch storeName + storeId
def get_store_list():
    try:
        cursor = store_details.find({}, {"storeName": 1, "_id": 1})
        stores = []
        for doc in cursor:
            name = doc.get("storeName")
            sid = doc.get("storeId")
            if name:
                stores.append({"storeName": name, "_id": sid})
        return stores
    except Exception as e:
        st.error(f"Error fetching stores: {e}")
        return []
stores = get_store_list()
if stores:
    # Unique + sorted
    store_names = sorted(list({s["storeName"] for s in stores}))

    selected_store = st.selectbox("Select Store", store_names)

    # Get the matching storeId
    selected_storeId = None
    for s in stores:
        if s["storeName"] == selected_store:
            selected_storeId = s["_id"]
            break

    st.write("Selected store:", selected_store)
    st.write("Store ID:", selected_storeId)
else:
    st.warning("No stores found in storeDetails collection.")

