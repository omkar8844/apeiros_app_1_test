# app.py
import streamlit as st
import pandas as pd
import json
import datetime
from typing import Optional, List, Dict

from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError
from bson import ObjectId
from bson.errors import InvalidId

# -----------------------
# Config / connection
# -----------------------
st.set_page_config(page_title="Apeiros Customer Support", layout="wide")

MONGO_URI = st.secrets["mongodb"]["uri"]

@st.cache_resource
def get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

def test_connection(client: MongoClient) -> bool:
    try:
        client.admin.command("ping")
        return True
    except Exception:
        return False

client = get_mongo_client()
if not test_connection(client):
    st.error("Failed to connect to MongoDB.")
    st.stop()

# DB / collections
db_retail = client["apeirosretail"]
store_details = db_retail["storeDetails"]
org_details = db_retail["organizationDetails"]

# NOTE: your bills DB/collection name, adjust if needed
db_bills = client["apeirosretaildataprocessing"]
bill_requests = db_bills["billRequest"]

# -----------------------
# Helpers
# -----------------------
@st.cache_data(ttl=300)
def get_store_list(limit: int = 10000) -> List[Dict]:
    """Return list of stores with _id and storeName."""
    try:
        cursor = store_details.find({}, {"_id": 1, "storeName": 1}).limit(limit)
        stores = []
        seen = set()
        for doc in cursor:
            name = doc.get("storeName") or "(No Name)"
            sid = doc.get("_id")
            label = name
            # ensure uniqueness by label; if duplicate, include id short
            if label in seen:
                # append short id to make label unique
                label = f"{name} ({str(sid)[:6]})"
            seen.add(label)
            stores.append({"storeName": name, "storeId": sid, "label": label})
        return stores
    except Exception as e:
        st.error(f"Error fetching store list: {e}")
        return []

def fetch_store_by_objid(objid: ObjectId) -> Optional[dict]:
    try:
        return store_details.find_one({"_id": objid})
    except Exception as e:
        st.error(f"Error fetching store document: {e}")
        return None

def fetch_org_by_tenant(tenant_value) -> Optional[dict]:
    """
    Attempt to find organizationDetails by tenantId.
    Tries direct match, string match, and ObjectId match.
    """
    try:
        # 1) direct match
        org = org_details.find_one({"tenantId": tenant_value})
        if org:
            return org
        # 2) string match
        org = org_details.find_one({"tenantId": str(tenant_value)})
        if org:
            return org
        # 3) if tenant_value is string, try ObjectId
        if isinstance(tenant_value, str):
            try:
                maybe_obj = ObjectId(tenant_value)
                org = org_details.find_one({"tenantId": maybe_obj})
                if org:
                    return org
            except InvalidId:
                pass
        return None
    except Exception as e:
        st.error(f"Error fetching organization: {e}")
        return None

def count_bills_for_store(store_objid: ObjectId) -> Optional[int]:
    """
    Count documents in bill_requests where storeId matches selected store id.
    Tries ObjectId match first, then string match.
    """
    try:
        if store_objid is None:
            return 0
        q_objid = {"storeId": store_objid}
        cnt = bill_requests.count_documents(q_objid)
        if cnt and cnt > 0:
            return cnt
        # fallback to string match
        cnt2 = bill_requests.count_documents({"storeId": str(store_objid)})
        return cnt2
    except PyMongoError as e:
        st.error(f"Error counting bills: {e}")
        return None

def format_date(dt):
    if isinstance(dt, datetime.datetime):
        # localize/format as needed; using day month year
        return dt.strftime("%d %B %Y")
    return str(dt)

def render_card(title: str, main_text: str, subtitle: Optional[str] = None, bg: str = "linear-gradient(135deg, #4B79A1, #283E51)"):
    """Small helper to render a consistent card."""
    subtitle_html = f'<div style="font-size:13px;opacity:0.85;">{subtitle}</div>' if subtitle else ""
    st.markdown(
        f"""
        <div style="
            padding: 14px;
            border-radius: 10px;
            text-align: center;
            background: {bg};
            color: #fff;
            box-shadow: 0 6px 18px rgba(0,0,0,0.12);
            min-height:86px;
            display:flex;
            flex-direction:column;
            justify-content:center;
        ">
            <div style="font-size:14px;font-weight:600;margin-bottom:6px;">{title}</div>
            {subtitle_html}
            <div style="font-size:28px;font-weight:700;margin-top:6px;color:#FFD700;">{main_text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# -----------------------
# UI
# -----------------------
st.title("Apeiros Customer Support")

# Left: Optional store table preview (collapsed)
with st.expander("Store list preview (first 100 rows)", expanded=False):
    try:
        preview_df = pd.DataFrame(list(store_details.find({}, {"storeName":1}).limit(100)))
        if not preview_df.empty:
            preview_df = preview_df.rename(columns={"_id": "storeId"})
            preview_df["storeId"] = preview_df["storeId"].astype(str)
            st.dataframe(preview_df)
        else:
            st.write("No stores to preview.")
    except Exception as e:
        st.write("Preview error:", e)

# Select store
stores = get_store_list()
if not stores:
    st.warning("No stores found in storeDetails or failed to load stores.")
    st.stop()

options = [s["label"] for s in stores]
selected_label = st.selectbox("Select store", options)

# map selection back to storeId and storeName
selected_store = next((s for s in stores if s["label"] == selected_label), None)
if not selected_store:
    st.warning("Selected store not found (unexpected).")
    st.stop()

selected_store_name = selected_store["storeName"]
selected_store_id = selected_store["storeId"]  # ObjectId

# Fetch full store document (no caching because ObjectId)
store_doc = fetch_store_by_objid(selected_store_id)

# Extract tenantId if present
tenant_id = None
if store_doc:
    tenant_id = store_doc.get("tenantId") or store_doc.get("tenant_id") \
                or store_doc.get("organization", {}).get("tenantId") \
                or store_doc.get("org", {}).get("tenantId")

# Build three cards in the requested order:
# 1) On-boarding Date (createdAt)
# 2) Phone number (organizationDetails -> tenantId)
# 3) Bill count

col1, col2, col3 = st.columns([1,1,1])

# 1) On-boarding Date
with col1:
    created_at_raw = store_doc.get("createdAt") if store_doc else None
    if created_at_raw:
        created_at = format_date(created_at_raw)
        render_card("On-boarding Date", created_at, subtitle=selected_store_name, bg="linear-gradient(90deg, #2c5364, #203a43, #0f2027)")
    else:
        render_card("On-boarding Date", "Not available", subtitle=selected_store_name, bg="linear-gradient(90deg,#555,#333)")

# 2) Phone number (organizationDetails via tenantId)
with col2:
    phone = None
    org_doc = None
    if tenant_id:
        org_doc = fetch_org_by_tenant(tenant_id)
    # fallback: try to match org by storeName
    if not org_doc and store_doc and store_doc.get("storeName"):
        try:
            org_doc = org_details.find_one({"name": store_doc.get("storeName")})
        except Exception:
            org_doc = None

    if org_doc:
        phone = org_doc.get("phoneNumber") or org_doc.get("phone") or org_doc.get("contactNumber") or org_doc.get("mobile")

    if phone:
        # clickable tel link (works on mobile / some desktops)
        phone_html = f'<a href="tel:{phone}" style="color:inherit;text-decoration:none;">{phone}</a>'
        render_card("Organization Phone", phone_html, subtitle="Tap to call", bg="linear-gradient(90deg,#0f2027,#203a43)")
    else:
        render_card("Organization Phone", "Not available", subtitle="tenantId missing or not found", bg="linear-gradient(90deg,#403b4a,#e7e9bb)")

# 3) Bill count
with col3:
    bill_count = count_bills_for_store(selected_store_id)
    if bill_count is None:
        render_card("Bills count", "Error", subtitle="Could not count bills", bg="linear-gradient(90deg,#8e2de2,#4a00e0)")
    else:
        render_card("Bills count", str(bill_count), subtitle="Total bills for this store", bg="linear-gradient(90deg,#4B79A1,#283E51)")

# -----------------------
# Optional: show recent bills table (toggle)
# -----------------------
if st.checkbox("Show recent bills for selected store"):
    try:
        # Fetch last 50 bills sorted by createdAt or _id desc
        # We attempt to match by ObjectId then by string
        q = {"storeId": selected_store_id}
        recent = list(bill_requests.find(q).sort([("_id", -1)]).limit(50))
        if not recent:
            # fallback to string match
            q2 = {"storeId": str(selected_store_id)}
            recent = list(bill_requests.find(q2).sort([("_id", -1)]).limit(50))

        if recent:
            for r in recent:
                # convert ObjectIds to strings for display
                if "_id" in r:
                    r["_id"] = str(r["_id"])
                if "storeId" in r and isinstance(r["storeId"], ObjectId):
                    r["storeId"] = str(r["storeId"])
            recent_df = pd.json_normalize(recent)
            st.dataframe(recent_df)
        else:
            st.info("No recent bills found for this store.")
    except Exception as e:
        st.error(f"Error fetching recent bills: {e}")
