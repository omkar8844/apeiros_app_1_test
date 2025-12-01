import streamlit as st
from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError
from bson import ObjectId
import pandas as pd
import json

st.set_page_config(page_title="MongoDB connection tester", layout="wide")
st.title("Test connection to Apeiros CosmosDB (MongoDB API)")

# Prefer secrets.toml; fallback to environment or manual input
uri = st.secrets.get("mongodb", {}).get("uri") if st.secrets else None

if not uri:
    st.warning("No MongoDB URI found in Streamlit secrets. You can paste it below (it will not be saved to secrets).")
    uri = st.text_input("MongoDB URI (mongodb+srv://...)", type="password")

st.markdown("---")

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Connection and query")
    db_name = st.text_input("Database name", value="apeirosretail")
    coll_name = st.text_input("Collection name", value="storeDetails")
    limit = st.number_input("Maximum documents to fetch", min_value=1, max_value=1000, value=100, step=10)
    fetch_btn = st.button("Fetch documents")

with col2:
    st.subheader("Options")
    hide_id = st.checkbox("Hide _id field in table", value=True)
    show_raw = st.checkbox("Show raw JSON output below", value=True)
    server_timeout = st.number_input("Server selection timeout (ms)", min_value=1000, max_value=30000, value=5000, step=500)


@st.cache_data(ttl=300)
def get_client(uri, timeout_ms):
    # For SRV URIs (mongodb+srv://...) MongoClient handles host/port automatically
    return MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)


@st.cache_data(ttl=300)
def fetch_store_list(uri, db_name, coll_name, timeout_ms=5000):
    """Return a list of dicts: [{"storeId": ..., "storeName": ...}, ...]"""
    client = None
    try:
        client = get_client(uri, timeout_ms)
        client.admin.command('ping')
        db = client[db_name]
        coll = db[coll_name]
        # fetch projection of storeId and storeName
        cursor = coll.find({}, {"storeId": 1, "storeName": 1}).limit(10000)
        stores = []
        seen = set()
        for d in cursor:
            sid = d.get('storeId')
            sname = d.get('storeName')
            # create a stable display label
            if sid is None and sname is None:
                continue
            label = f"{sname} ({sid})" if sname and sid else (sname or str(sid))
            if label in seen:
                continue
            seen.add(label)
            stores.append({"storeId": sid, "storeName": sname, "label": label})
        return stores
    except Exception as e:
        st.error(f"Error fetching store list: {e}")
        return []
    finally:
        try:
            if client:
                client.close()
        except Exception:
            pass


def count_bills_for_store(uri, db_name_bills, bills_coll_name, store_id, timeout_ms=5000):
    client = None
    try:
        client = get_client(uri, timeout_ms)
        client.admin.command('ping')
        db = client[db_name_bills]
        coll = db[bills_coll_name]
        if store_id is None:
            # count bills where storeId is null
            q = {"storeId": {"$exists": False}}
        else:
            q = {"storeId": store_id}
        return coll.count_documents(q)
    except Exception as e:
        st.error(f"Error counting bills: {e}")
        return None
    finally:
        try:
            if client:
                client.close()
        except Exception:
            pass


# --- New UI for cross-collection aggregation (your request) ---
st.markdown("## Store → Bill count (cross-collection)")
col_a, col_b = st.columns([3, 2])
with col_a:
    st.write("Select the source database/collection that holds stores (default: apeirosretail.storeDetails)")
    stores_db = st.text_input("Stores DB", value="apeirosretail")
    stores_coll = st.text_input("Stores collection", value="storeDetails")

with col_b:
    st.write("Select the bills database/collection that holds bills (default: apeirosdataprocessing.billRequests)")
    bills_db = st.text_input("Bills DB", value="apeirosdataprocessing")
    bills_coll = st.text_input("Bills collection", value="billRequests")

# Fetch store list (cached)
if uri:
    with st.spinner("Loading stores..."):
        stores = fetch_store_list(uri, stores_db, stores_coll, int(server_timeout))
else:
    stores = []

if not stores:
    st.info("No stores found (or connection not configured). Make sure your URI is in Streamlit secrets and the DB/collection names are correct.")

# Build dropdown options
options = [s['label'] for s in stores]
selected_label = st.selectbox("Select store", options=options if options else ["(no stores)"])

selected_store = None
if stores and selected_label and selected_label != "(no stores)":
    # find the matching store dict
    for s in stores:
        if s['label'] == selected_label:
            selected_store = s
            break

if selected_store:
    st.markdown(f"**Selected:** {selected_store['label']}")
    # count bills
    with st.spinner("Counting bills for selected store..."):
        cnt = count_bills_for_store(uri, bills_db, bills_coll, selected_store['storeId'], int(server_timeout))
    if cnt is not None:
        st.metric(label="Number of bills (billId) for this store", value=cnt)

st.markdown("---")

# --- Original fetch documents behavior retained ---
if fetch_btn:
    if not uri:
        st.error("Please provide a MongoDB URI (or add it to Streamlit secrets).")
    else:
        with st.spinner("Connecting to MongoDB..."):
            try:
                client = get_client(uri, server_timeout)
                # force server selection / connect
                client.admin.command('ping')
            except ServerSelectionTimeoutError as e:
                st.error(f"Could not connect to server: {e}")
            except PyMongoError as e:
                st.error(f"MongoDB error: {e}")
            else:
                try:
                    db = client[db_name]
                    coll = db[coll_name]
                    cursor = coll.find({}).limit(int(limit))
                    docs = list(cursor)

                    if len(docs) == 0:
                        st.info("Query succeeded but returned 0 documents.")
                    else:
                        # Optionally hide ObjectId
                        if hide_id:
                            for d in docs:
                                if "_id" in d:
                                    d["_id"] = str(d["_id"])  # convert to string if present

                        # Show preview table using pandas
                        try:
                            df = pd.json_normalize(docs)
                            st.subheader(f"Sample documents ({min(len(docs), 100)} shown)")
                            st.dataframe(df)
                        except Exception:
                            st.write("Couldn't normalize documents into a table. Showing raw JSON below.")

                        if show_raw:
                            st.subheader("Raw JSON documents")
                            st.code(json.dumps(docs, default=str, indent=2))

                except PyMongoError as e:
                    st.error(f"Failed fetching documents: {e}")
                finally:
                    try:
                        client.close()
                    except Exception:
                        pass
