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

db_bills = client["apeirosretaildataprocessing"]
bill_requests = db_bills["billRequest"]

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
        org = org_details.find_one({"tenantId": tenant_value})
        if org:
            return org
        org = org_details.find_one({"tenantId": str(tenant_value)})
        if org:
            return org
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

st.title("Apeiros Customer Support")



# Select store
stores = get_store_list()
if not stores:
    st.warning("No stores found in storeDetails or failed to load stores.")
    st.stop()

options = [s["label"] for s in stores]
selected_label = st.selectbox("Select store", options)

selected_store = next((s for s in stores if s["label"] == selected_label), None)
if not selected_store:
    st.warning("Selected store not found (unexpected).")
    st.stop()

selected_store_name = selected_store["storeName"]
selected_store_id = selected_store["storeId"]  
store_doc = fetch_store_by_objid(selected_store_id)


tenant_id = None
if store_doc:
    tenant_id = store_doc.get("tenantId") or store_doc.get("tenant_id") \
                or store_doc.get("organization", {}).get("tenantId") \
                or store_doc.get("org", {}).get("tenantId")


col1, col2, col3 = st.columns([1,1,1])

with col1:
    created_at_raw = store_doc.get("createdAt") if store_doc else None
    if created_at_raw:
        created_at = format_date(created_at_raw)
        render_card("On-boarding Date", created_at, subtitle=selected_store_name, bg="linear-gradient(90deg, #2c5364, #203a43, #0f2027)")
    else:
        render_card("On-boarding Date", "Not available", subtitle=selected_store_name, bg="linear-gradient(90deg,#555,#333)")


with col2:
    phone = None
    org_doc = None
    if tenant_id:
        org_doc = fetch_org_by_tenant(tenant_id)

    if not org_doc and store_doc and store_doc.get("storeName"):
        try:
            org_doc = org_details.find_one({"name": store_doc.get("storeName")})
        except Exception:
            org_doc = None

    if org_doc:
        phone = org_doc.get("phoneNumber") or org_doc.get("phone") or org_doc.get("contactNumber") or org_doc.get("mobile")

    if phone:

        phone_html = f'<a href="tel:{phone}" style="color:inherit;text-decoration:none;">{phone}</a>'
        render_card("Organization Phone", phone_html, subtitle="Tap to call", bg="linear-gradient(90deg,#0f2027,#203a43)")
    else:
        render_card("Organization Phone", "Not available", subtitle="tenantId missing or not found", bg="linear-gradient(90deg,#403b4a,#e7e9bb)")

with col3:
    bill_count = count_bills_for_store(selected_store_id)
    if bill_count is None:
        render_card("Bills count", "Error", subtitle="Could not count bills", bg="linear-gradient(90deg,#8e2de2,#4a00e0)")
    else:
        render_card("Bills count", str(bill_count), subtitle="Total bills for this store", bg="linear-gradient(90deg,#4B79A1,#283E51)")


if st.checkbox("Show recent bills for selected store"):
    try:

        q = {"storeId": selected_store_id}
        recent = list(bill_requests.find(q).sort([("_id", -1)]).limit(50))
        if not recent:
       
            q2 = {"storeId": str(selected_store_id)}
            recent = list(bill_requests.find(q2).sort([("_id", -1)]).limit(50))

        if recent:
            for r in recent:
     
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

# -----------------------
# Wallet stats from promotionalMessageCredit
# -----------------------
def safe_num(x):
    """Convert various types to float safely, fallback 0."""
    try:
        if x is None:
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        # handle numeric strings with commas
        if isinstance(x, str):
            x_clean = x.replace(",", "").strip()
            return float(x_clean) if x_clean != "" else 0.0
    except Exception:
        return 0.0
    return 0.0

def get_wallet_stats_by_tenant(tenant_value):
    """
    Returns dict: {'balance': sum(lifetimeCredits), 'consumption': sum(lifetimeConsumption)}
    Tries direct tenantValue match, string match, and ObjectId match (if tenant_value is str).
    """
    db_cm = client["apeirosretailcustomermanagement"]
    coll = db_cm["promotionalMessageCredit"]
    try:
        # Try direct match
        cursor = list(coll.find({"tenantId": tenant_value}))
        if not cursor and tenant_value is not None:
            # Try string match
            cursor = list(coll.find({"tenantId": str(tenant_value)}))

        if not cursor and isinstance(tenant_value, str):
            # Try converting to ObjectId if possible
            try:
                tid_obj = ObjectId(tenant_value)
                cursor = list(coll.find({"tenantId": tid_obj}))
            except Exception:
                pass

        # If still empty, return zeros
        if not cursor:
            return {"balance": 0.0, "consumption": 0.0, "docs_count": 0}

        total_credits = 0.0
        total_consumption = 0.0
        for d in cursor:
            total_credits += safe_num(d.get("lifetimeCredits"))
            total_consumption += safe_num(d.get("lifetimeConsumption"))

        return {"balance": total_credits, "consumption": total_consumption, "docs_count": len(cursor)}
    except Exception as e:
        st.error(f"Error fetching wallet stats: {e}")
        return {"balance": 0.0, "consumption": 0.0, "docs_count": 0}

# Only run if we have tenant_id (otherwise try to derive)
wallet_balance = None
wallet_consumption = None
wallet_docs = 0

if tenant_id:
    stats = get_wallet_stats_by_tenant(tenant_id)
    wallet_balance = stats["balance"]
    wallet_consumption = stats["consumption"]
    wallet_docs = stats["docs_count"]
else:
    # Attempt to derive tenant_id from store_doc fields (extra try)
    derived_tenant = None
    if store_doc:
        derived_tenant = store_doc.get("tenantId") or store_doc.get("tenant_id") or store_doc.get("organization", {}).get("tenantId")
    if derived_tenant:
        stats = get_wallet_stats_by_tenant(derived_tenant)
        wallet_balance = stats["balance"]
        wallet_consumption = stats["consumption"]
        wallet_docs = stats["docs_count"]

# Render the two wallet cards in a new row under existing cards
st.markdown("---")
st.subheader("Customer Wallet (promotional)")

wcol1, wcol2 = st.columns([1, 1])

with wcol1:
    if wallet_balance is None:
        render_card("Wallet Balance", "Not available", subtitle="tenantId missing or not found", bg="linear-gradient(90deg,#283E51,#4B79A1)")
    else:
        # Format nicely with 2 decimal places, strip .00 if integer-looking
        def fmt(x):
            if abs(x - round(x)) < 1e-9:
                return f"{int(round(x))}"
            return f"{x:,.2f}"
        render_card("Wallet Balance", fmt(wallet_balance), subtitle=f"{wallet_docs} promo doc(s) matched", bg="linear-gradient(90deg,#0f2027,#203a43)")

with wcol2:
    if wallet_consumption is None:
        render_card("Wallet Consumption", "Not available", subtitle="tenantId missing or not found", bg="linear-gradient(90deg,#8e2de2,#4a00e0)")
    else:
        render_card("Wallet Consumption", fmt(wallet_consumption), subtitle=f"{wallet_docs} promo doc(s) matched", bg="linear-gradient(90deg,#4B79A1,#283E51)")

