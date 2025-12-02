import streamlit as st
from pymongo import MongoClient
import pandas as pd
import datetime
from datetime import datetime
import altair as alt
# Custom HTML/CSS for KPI cards
def styled_metric(label, value, bg_color="#2E86C1", font_color="#FFFFFF", label_size="20px", value_size="32px"):
    st.markdown(
        f"""
        <div style="
            background-color: {bg_color};
            padding: 20px;
            border-radius: 10px;
            text-align: center;
        ">
            <div style="font-size: {label_size}; color: {font_color}; font-weight: bold;">
                {label}
            </div>
            <div style="font-size: {value_size}; color: {font_color}; font-weight: bold;">
                {value}
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

mongo_uri = st.secrets["mongo"]["uri"]

client=MongoClient(mongo_uri)
db_retail=client['apeirosretail']
storedetails_collection=db_retail['storeDetails']
db_bills=client['apeirosretaildataprocessing']
billReq=db_bills['billRequest']
store_names=storedetails_collection.distinct("storeName")
ind=store_names.index('SAARA')
st.title("Today's Bill Count")
today = datetime.today()
start = datetime(today.year, today.month, today.day)
end = datetime(today.year, today.month, today.day, 23, 59, 59)
bill_docs_bar = list(billReq.find({"createdAt": {"$gte": start, "$lte": end}},{"billId": 1, "storeId": 1, "_id": 0}))
today_bill_df=(pd.DataFrame(bill_docs_bar))
store_ids_bar = today_bill_df["storeId"].unique().tolist()
store_map=[]
#({'billId': {'$in': bill_ids}}))
for i in list(storedetails_collection.find({'_id':{'$in':store_ids_bar}})):
    store_map.append({
        "storeId":i['_id'],
        "storeName":i['storeName']
        })
#store_map = {doc["_id"]: doc["storeName"] for doc in storedetails_collection.find()}
store_map_df=(pd.DataFrame(store_map))
today_bill_df=today_bill_df.merge(store_map_df,on='storeId',how='inner')
st.write("Today's Bill Count- ",today_bill_df['billId'].nunique())

bill_count_df = (
    today_bill_df.groupby("storeName")["billId"]
    .count()
    .reset_index()
    .rename(columns={"billId": "billCount"})
)
chart = (
    alt.Chart(bill_count_df)
    .mark_bar()
    .encode(
        x=alt.X("storeName:N", sort="-y", title="Store Name"),
        y=alt.Y("billCount:Q", title="Number of Bills"),
        tooltip=["storeName", "billCount"]
    )
    .properties(height=400)
)

st.altair_chart(chart, use_container_width=True)
#------------------------------------------------------------------
st.title("Store Insights 📺")
selected_store=st.selectbox("Choose a store",store_names,index=ind)
if selected_store:
    #Org Processing
    org=db_retail['organizationDetails']
    #Bill Request Processing

    #Getting selected stores data
    docs = list(storedetails_collection.find({"storeName": selected_store}))
    #Getting storeId
    for doc in docs:
        storeId=doc['_id']
    #Getting tenantId
    for doc2 in docs:
        tenantId=doc2['tenantId']
    #Getting onboard date
    for doc3 in docs:
        createdAt=doc3['createdAt']
    onboard_date=createdAt.strftime('%d %B %Y') 
    
    #Getting respective org    
    org_doc=list(org.find({'tenantId':tenantId}))
    
    #Getting respective bill
    bill_doc=list(billReq.find({'storeId':storeId}))
    
    #Getting respective bill invoice extracted data
    bill_ids=[i['billId'] for i in bill_doc]
    in_ex=db_bills['invoiceExtractedData']
    in_ex_docs = list(in_ex.find({'billId': {'$in': bill_ids}}))
    total_in_amount=0
    total_rec_amount=0
    total_trans_amount=0
    if in_ex_docs:
        total_in_amount = sum(
            float(i['InvoiceTotal']['value'])
            for i in in_ex_docs
            if 'InvoiceTotal' in i
            and 'value' in i['InvoiceTotal']
            and i['InvoiceTotal']['value'] is not None
            and i['InvoiceTotal']['value'] != ""
        )
     
#Getting respective bills from rec extracted
    rec_ex=db_bills['receiptExtractedData'] 
    rec_ex_docs=list(rec_ex.find({'billId':{'$in':bill_ids}})) 
    if rec_ex_docs:
        total_rec_amount=sum(
            float(i['Total']['value'])for i in rec_ex_docs
                  if i['Total']['value'] is not None
                  and i['Total']['value']!="")
        
        
#Getting respective bills from bill_trans
    trans_bill=db_bills['billtransactions']
    trans_bill_docs=list(trans_bill.find({'billId':{'$in':bill_ids}}))
    if trans_bill_docs:
        total_trans_amount=sum(
            float(i['billAmount']) for i in trans_bill_docs
            if i['billAmount'] is not None
            and i['billAmount']!=""
        )
        
    #Getting total rev
    final_total_rev=int(total_rec_amount+total_trans_amount+total_in_amount)
    
    #Getting Wallet Data
    db_wallet=client['apeirosretailcustomermanagement']
    wallet_collection=db_wallet['promotionalMessageCredit']
    wallet_doc=list(wallet_collection.find({'tenantId':tenantId}))
    wallet_bal=[i['currentAvailable'] for i in wallet_doc]
    wallet_cons=[i['lifetimeConsumption'] for i in wallet_doc]
    wallet_balance=round(wallet_bal[0],2)
    wallet_consuption=round(wallet_cons[0],2)

    #Getting bill ID count
    bill_count=len(set(bill_ids))
    
    #Getting Phone number
    phone_list=[i['phoneNumber'] for i in org_doc]
    phone_value=phone_list[0][0]

    #Plotting the results
    a,b=st.columns(2,gap="small")
    with a:
        styled_metric("Phone Number 📞", phone_value, bg_color="#34495E", font_color="#F1C40F", label_size="20px", value_size="28px")
    with b:
        styled_metric("Onboard Date ✈️", onboard_date, bg_color="#34495E", font_color="#F1C40F", label_size="20px", value_size="28px")
    c,d=st.columns(2,gap="small")
    with c:
        st.space(size="small") 
        styled_metric("Bill Count 🧾", bill_count, bg_color="#27AE60", font_color="#FFFFFF", label_size="20px", value_size="28px")
    with d:
        st.space(size="small") 
        styled_metric("Total Revenue 📈", final_total_rev, bg_color="#27AE60", font_color="#FFFFFF", label_size="20px", value_size="28px")

    st.subheader("Wallet Information")   
    e,f=st.columns(2,gap='small')
    with e:
        styled_metric("Wallet Balance 💼", wallet_balance, bg_color="#34495E", font_color="#F1C40F", label_size="20px", value_size="28px")
    with f:
        styled_metric("Wallet Consumption ⚡", wallet_consuption, bg_color="#34495E", font_color="#F1C40F", label_size="20px", value_size="28px")
        
        
    #Creating show bills button
    show_bills = st.checkbox("Show Bills") 
    if show_bills: 
        st.dataframe(bill_doc)



#---------------------------------------------        
#preparing for bar graph
# today = datetime.today()
# start = datetime(today.year, today.month, today.day)
# end = datetime(today.year, today.month, today.day, 23, 59, 59)
# bill_docs_bar = list(billReq.find({"createdAt": {"$gte": start, "$lte": end}},{"billId": 1, "storeId": 1, "_id": 0}))
# today_bill_df=(pd.DataFrame(bill_docs_bar))
# store_ids_bar = today_bill_df["storeId"].unique().tolist()
# store_map=[]
# #({'billId': {'$in': bill_ids}}))
# for i in list(storedetails_collection.find({'_id':{'$in':store_ids_bar}})):
#     store_map.append({
#         "storeId":i['_id'],
#         "storeName":i['storeName']
#         })
# #store_map = {doc["_id"]: doc["storeName"] for doc in storedetails_collection.find()}
# store_map_df=(pd.DataFrame(store_map))
# today_bill_df=today_bill_df.merge(store_map_df,on='storeId',how='inner')

# bill_count_df = (
#     today_bill_df.groupby("storeName")["billId"]
#     .count()
#     .reset_index()
#     .rename(columns={"billId": "billCount"})
# )
# # st.bar_chart(
#     data=bill_count_df,
#     x="storeName",
#     y="billCount",
# )
# import altair as alt

# chart = (
#     alt.Chart(bill_count_df)
#     .mark_bar()
#     .encode(
#         x=alt.X("storeName:N", sort="-y", title="Store Name"),
#         y=alt.Y("billCount:Q", title="Number of Bills"),
#         tooltip=["storeName", "billCount"]
#     )
#     .properties(height=400)
# )

# st.altair_chart(chart, use_container_width=True)
