import streamlit as st
from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError
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


def get_client(uri, timeout_ms):
    # For SRV URIs (mongodb+srv://...) MongoClient handles host/port automatically
    return MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)


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
