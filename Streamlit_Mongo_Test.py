import streamlit as st
from pymongo import MongoClient
import pandas as pd
import datetime as dtm
from datetime import datetime, date, timedelta
import altair as alt

#HTML BLOCKS
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
    
#Mongo Connection
mongo_uri = st.secrets.get("mongodb", {}).get("uri") if st.secrets else None
#mongo_uri = st.secrets["mongo"]["uri"]
client=MongoClient(mongo_uri)
#Database Connectons
db_retail=client['apeirosretail']
db_bills=client['apeirosretaildataprocessing']
db_wallet=client['apeirosretailcustomermanagement']
#Collection Connections
#1
storedetails_collection=db_retail['storeDetails']
org=db_retail['organizationDetails']
#2
billReq=db_bills['billRequest']
in_ex=db_bills['invoiceExtractedData']
rec_ex=db_bills['receiptExtractedData']
trans_bill=db_bills['billtransactions']
#3
wallet_collection=db_wallet['promotionalMessageCredit']
#4
payment_dt=db_retail['paymentDetails']

#---------------------------------------------------------------------------------------------------------------
# #date filter 


# date_range=st.date_input("When's your birthday", dtm.date(2019, 7, 6))
# #Visual total onboards with filter

# pipeline = [
#     {"$group": {"_id": "$_id"}},   # group by _id
#     {"$count": "distinctCount"}    # count groups
# ]
# store_count_dict=list(storedetails_collection.aggregate(pipeline))
# st.write(store_count_dict[0]["distinctCount"])
#------------------------------------------------------------------------------
#Visual Daily bill count bar graph
st.title("Today's Bill Count")
today = datetime.today()
start = datetime(today.year, today.month, today.day)
end = datetime(today.year, today.month, today.day, 23, 59, 59)

bill_docs_bar = list(billReq.find({"createdAt": {"$gte": start, "$lte": end}},{"billId": 1, "storeId": 1, "_id": 0}))
if bill_docs_bar:
    today_bill_df=(pd.DataFrame(bill_docs_bar))
    store_ids_bar = today_bill_df["storeId"].unique().tolist()
    store_map=[]
    #({'billId': {'$in': bill_ids}}))
    for i in list(storedetails_collection.find({'_id':{'$in':store_ids_bar}},{"_id":1,"storeName":1})):
        store_map.append({
            "storeId":i['_id'],
            "storeName":i['storeName']
            })
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
else:
    st.subheader('No Bills Till Now')

#--------------------------------------------------------------------------------------
#Store insights
st.title("Store Insights 📺")
store_names=storedetails_collection.distinct("storeName")
ind=store_names.index('HP World Panvel')
#Select store widget
selected_store=st.selectbox("Choose a store",store_names,index=ind)
if selected_store:
    store_doc=list(storedetails_collection.find({"storeName": selected_store},{'_id':1,'storeName':1,'tenantId':1,'createdAt':1}))
    for doc in store_doc:
        storeId=doc['_id']
    #Getting tenantId
    for doc2 in store_doc:
        tenantId=doc2['tenantId']
    #Getting onboard date
    for doc3 in store_doc:
        createdAt=doc3['createdAt']
    onboard_date=createdAt.strftime('%d %B %Y') 
    #Getting respective org
    org_doc=list(org.find({'tenantId':tenantId},{'tenantId':1,'phoneNumber':1}))
    #Getting Phone number
    phone_list=[i['phoneNumber'] for i in org_doc]
    phone_value=phone_list[0][0]
    
    #for Bill amount sum
    total_in_amount=0
    total_rec_amount=0
    total_trans_amount=0
    #getting respective bills
    bill_doc=list(billReq.find({'storeId':storeId},{'storeId':1,'billId':1,'createdAt':1,'name':1}))
    bill_ids=[i['billId'] for i in bill_doc]    
    #Getting bill ID count
    bill_count=len(set(bill_ids))
    #Getting respective bill invoice extracted data
    in_ex_docs = list(in_ex.find({'billId': {'$in': bill_ids}},{"billId":1,"InvoiceTotal":1,"_id":0}))
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
    rec_ex_docs=list(rec_ex.find({'billId':{'$in':bill_ids}},{'billId':1,'Total':1,'_id':0}))
    if rec_ex_docs:
        total_rec_amount=sum(
            float(i['Total']['value'])for i in rec_ex_docs
                  if i['Total']['value'] is not None
                  and i['Total']['value']!="")
    #Getting respective bills from bill_trans
    trans_bill_docs=list(trans_bill.find({'billId':{'$in':bill_ids}},{'billId':1,'billAmount':1}))
    if trans_bill_docs:
        total_trans_amount=sum(
            float(i['billAmount']) for i in trans_bill_docs
            if i['billAmount'] is not None
            and i['billAmount']!=""
        )
    
    #Getting total rev
    final_total_rev=int(total_rec_amount+total_trans_amount+total_in_amount)
    
    #Getting Wallet Data
    wallet_collection=db_wallet['promotionalMessageCredit']
    wallet_doc=list(wallet_collection.find({'tenantId':tenantId},{'tenantId':1,'currentAvailable':1,'lifetimeConsumption':1,'_id':0}))  
    wallet_bal=[i['currentAvailable'] for i in wallet_doc]
    wallet_cons=[i['lifetimeConsumption'] for i in wallet_doc]
    if wallet_bal:
        wallet_balance=round(wallet_bal[0],2)
    else:
        wallet_balance=0
    if wallet_cons:
        wallet_consuption=round(wallet_cons[0],2)
    else:
        wallet_consuption=0
    
    #Payment related reports
    payment_doc=(list(payment_dt.find({'storeId':storeId,"transactionStatus":"success"},{'tenantId':1,'payment_id':1,"transactionStatus":1,"requestType":1,"storeId":1,"netAmount":1,"packageName":1})))    
    #net_am
    nt_list=[]
    #st.write(payment_doc)
    if payment_doc:
        try:
            net_amt=[i['netAmount'] for i in payment_doc]
            for i in net_amt:
                if i is not None:
                    nt_list.append(float(i))
            if len(nt_list)>0:
                nt=sum(nt_list)
            else:
                nt=0
        except KeyError:
            nt=0
    else:
        nt=0
    pcg=[i["packageName"] for i in payment_doc]
    if len(pcg)>0:
        for i in pcg:
            if i=="</div>":
                pcg_name="No Record"
            else:
                pcg_name=(i)
    else:
        pcg_name='No record'
        
    #Daily Bill Count
    todyas_bills=list(billReq.find({
    'storeId': storeId,
    'createdAt': {
        '$gte': start,
        '$lte': end
    }
    },{'billId':1,'_id':0})) 
    td_bill_count=len(todyas_bills)
    
    #Plotting the results
    z,=st.columns(1)
    with z:
        st.space(size="small") 
        styled_metric("Today's Bill Count 🧾", td_bill_count, bg_color="#27AE60", font_color="#FFFFFF", label_size="20px", value_size="28px")
    a,b=st.columns(2,gap="small")
    with a:
        st.space(size="small")
        styled_metric("Phone Number 📞", phone_value, bg_color="#34495E", font_color="#F1C40F", label_size="20px", value_size="28px")
    with b:
        st.space(size="small")
        styled_metric("Onboard Date ✈️", onboard_date, bg_color="#34495E", font_color="#F1C40F", label_size="20px", value_size="28px")
    c,d=st.columns(2,gap="small")
    with c:
        st.space(size="small") 
        styled_metric("Bill Count 🧾", bill_count, bg_color="#27AE60", font_color="#FFFFFF", label_size="20px", value_size="28px")
    with d:
        st.space(size="small") 
        styled_metric("Total Revenue 📈", final_total_rev, bg_color="#27AE60", font_color="#FFFFFF", label_size="20px", value_size="28px")
    st.subheader("Wallet Information")  
    e,f= st.columns(2,gap="small")
    with e:
        st.space(size="small")
        styled_metric("Wallet Balance 💼", wallet_balance, bg_color="#34495E", font_color="#F1C40F", label_size="20px", value_size="28px")
    with f:
        st.space(size="small")
        styled_metric("Wallet Consumption ⚡", wallet_consuption, bg_color="#34495E", font_color="#F1C40F", label_size="20px", value_size="28px")
    
    g,h=st.columns(2,gap='small')
    with g:
        st.space(size="small")
        styled_metric("Total Payment 💵",nt, bg_color="#34495E", font_color="#F1C40F", label_size="20px", value_size="28px")
    with h:
        st.space(size="small")
        styled_metric("Package Name 📦",pcg_name, bg_color="#34495E", font_color="#F1C40F", label_size="20px", value_size="28px")
    
        
    #Creating show bills button
    show_bills = st.checkbox("Show Bills") 
    if show_bills: 
        st.dataframe(bill_doc)
    
    
    
    
    
    
    
    
    
    
    
