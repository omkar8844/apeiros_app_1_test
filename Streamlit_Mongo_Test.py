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
    st.success("Connected to Apeiros MongoDB successfully!")
else:
    st.error("Failed to connect to MongoDB.")


if test_connection(client):

    db_retail = client["apeirosretail"]
    store_details = db_retail["storeDetails"]

    db_bills = client["apeirosretaildataprocessing"]
    bill_requests = db_bills["billRequest"]

    st.write("Databases ready to use!")
else:
    st.error("Could not connect to database.")

st.title("Apeiros Customer Support")
store_df=pd.DataFrame(list(store_details.find()))
store_df=store_df.rename(columns={'_id':'storeId'})
# bill_df=pd.DataFrame(list(bill_requests.find())) 
# bill_df=bill_df.merge(store_df[['storeId','storeName']],on='storeId',how='inner')
# st.dataframe(bill_df)
# Fetch storeName + storeId
def get_store_list():
    try:
        cursor = store_details.find({}, {"_id": 1, "storeName": 1})
        stores = []
        for doc in cursor:
            name = doc.get("storeName", "(No Name)")
            sid = doc["_id"]   # use ObjectId
            stores.append({"storeName": name, "storeId": sid})
        return stores
    except Exception as e:
        st.error(f"Error fetching stores: {e}")
        return []
stores = get_store_list()

if stores:
    store_names = [s["storeName"] for s in stores]

    selected_store = st.selectbox("Select Store", store_names)

    # Find ObjectId
    selected_storeId = next(
        (s["storeId"] for s in stores if s["storeName"] == selected_store),
        None
    )

    st.write("Selected Store:", selected_store)
else:
    st.warning("No stores found.")

def count_bills_for_store(store_objid):
    """
    Count bill documents in bill_requests where the storeId matches store_objid.
    Tries both ObjectId match and string match to be robust.
    """
    try:
        # First try matching as an ObjectId (most likely)
        q_objid = {"storeId": store_objid}
        cnt = bill_requests.count_documents(q_objid)
        if cnt and cnt > 0:
            return cnt

        # If no results, try matching the string form (in case storeId stored as string)
        q_str = {"storeId": str(store_objid)}
        cnt2 = bill_requests.count_documents(q_str)
        return cnt2
    except PyMongoError as e:
        st.error(f"Error counting bills: {e}")
        return None

# Only run when a store is selected
if selected_storeId is not None:
    # selected_storeId is an ObjectId (from store_details._id) 
    bill_count = count_bills_for_store(selected_storeId)

    # If still None or zero, show helpful messages
    if bill_count is None:
        st.warning("Could not count bills due to an error.")
    else:
        st.markdown(
    f"""
    <div style="
        background: linear-gradient(135deg, #4B79A1, #283E51);
        padding: 20px;
        border-radius: 12px;
        text-align: center;
        color: white;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
        margin-top: 20px;
    ">
        <h3 style="margin-bottom: 10px;">Bills count for {selected_store}</h3>
        <h1 style="
            font-size: 48px;
            margin: 0;
            font-weight: 800;
            color: #FFD700;
            text-shadow: 0 0 12px rgba(255,215,0,0.8);
        ">{bill_count}</h1>
    </div>
    """,
    unsafe_allow_html=True
)
else:
    st.info("Selected store has no _id or store id could not be determined.")

