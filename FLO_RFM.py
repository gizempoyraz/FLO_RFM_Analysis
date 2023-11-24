###############################################################
# (Customer Segmentation with RFM)
###############################################################



# reading flo_data_20K.csv data
import pandas as pd
import datetime as dt

df_ = pd.read_csv("datasets\flo_data_20k.csv")
df = df_.copy()

#Top 10 observations
df.head(10)

#Variable names
df.columns

#Descriptive statistics
df.describe().T

#Null value
df.isnull().sum()
df.isnull().sum().any()

#Variable types, review
df.info()

#3. Omnichannel means that customers shop both online and offline platforms. Total for each customer
# new variables created for number of purchases and spending

df["total_order_num"] = df["order_num_total_ever_offline"] + df["order_num_total_ever_online"]
df["total_value_online"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_online"]
df["total_value_offline"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_offline"]
df["total_value"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]
df.info()

#Variable types were examined. The type of variables expressing date was changed to date.
date_columns = df.columns[df.columns.str.contains("date")]
df[date_columns] = df[date_columns].apply(pd.to_datetime)

#Looking at the distribution of the number of customers, average number of products purchased and total expenses in shopping channels
df.groupby("order_channel").agg({"master_id": "count",
                                 "total_order_num": "sum",
                                 "total_value": "sum"})

#Top 10 customers with the most revenue
df.sort_values("total_value", ascending=False).head(10)
# df.sort_values("total_value",ascending=False)[:10]

#Top 10 customers with the most orders
df.sort_values("total_order_num", ascending=False).head(10)


# df.sort_values("total_order_num", ascending=False)[:10]

#Functionalized version of the data preparation process

def data_prep(dataframe):
    dataframe["total_order_num"] = dataframe["order_num_total_ever_offline"] + dataframe["order_num_total_ever_online"]
    dataframe["total_value_online"] = dataframe["customer_value_total_ever_online"] + dataframe[
        "customer_value_total_ever_online"]
    dataframe["total_value_offline"] = dataframe["customer_value_total_ever_offline"] + dataframe[
        "customer_value_total_ever_offline"]
    dataframe["total_value"] = dataframe["customer_value_total_ever_offline"] + dataframe[
        "customer_value_total_ever_online"]

    date_columns = df.columns[df.columns.str.contains("date")]
    df[date_columns] = df[date_columns].apply(pd.to.datetime)

    order_channel_summary = dataframe.groupby("order_channel").agg({"master_id": "count",
                                                                    "total_order_num": "sum",
                                                                    "total_value": "sum"})

    top_10_total_value_customer = dataframe.sort_values("total_value", ascending=False)
    top_10_total_order_num_customer = dataframe.sort_values("total_order_num", ascending=False)

    return order_channel_summary, top_10_total_value_customer, top_10_total_order_num_customer


#Calculation of RFM Metrics

df["last_order_date"].max()  # Timestamp('2021-05-30 00:00:00')

analysis_date = dt.datetime(2021, 6, 1)
rfm = pd.DataFrame()

rfm["customer_id"] = df["master_id"]

"""
rfm = df.groupby("master_id").agg(
    {"last_order_date": lambda last_order_date: (analysis_date - last_order_date.max()).days,
     "total_order_num": "sum",
     "total_value": "sum"})

     rfm.columns = ["recency", "frequency", "monetary"]
"""
rfm["recency"] = (analysis_date - df["last_order_date"]).astype("timedelta64[D]")
rfm["frequency"] = df["total_order_num"]
rfm["monetary"] = df["total_value"]

#Calculation of RF and RFM Scores

rfm["recency_score"] = pd.qcut(rfm["recency"], 5, [5, 4, 3, 2, 1])
rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, [1, 2, 3, 4, 5])
rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, [1, 2, 3, 4, 5])

rfm.head()

rfm["rfm_score"] = rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str)

#Defining RF Scores as Segments

#for refex version
seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'}

#if you dont want to use regex
import re


def map_segment(segment):
    if re.match(r'[1-2][1-2]', segment):
        return 'hibernating'
    elif re.match(r'[1-2][3-4]', segment):
        return 'at_risk'
    elif re.match(r'[1-2]5', segment):
        return 'cant_loose'
    elif re.match(r'3[1-2]', segment):
        return 'about_to_sleep'
    elif re.match(r'33', segment):
        return 'need_attention'
    elif re.match(r'[3-4][4-5]', segment):
        return 'loyal_customers'
    elif re.match(r'41', segment):
        return 'promising'
    elif re.match(r'51', segment):
        return 'new_customers'
    elif re.match(r'[4-5][2-3]', segment):
        return 'potential_loyalists'
    elif re.match(r'5[4-5]', segment):
        return 'champions'
    else:
        return 'unknown_segment'

#for regex
rfm["segment"] = rfm["rfm_score"].replace(seg_map, regex=True)

#for the second way
#rfm["segment"] = rfm["rfm_score"].apply(lambda x: map_segment(x) if pd.notnull(x) else x)
rfm["segment"] = rfm["rfm_score"].apply(lambda x: map_segment(x))

rfm[rfm["rfm_score"] == "31"].head()
rfm[rfm["rfm_score"] == "55"][:12]


#Examining the recency, frequency and monetary averages of the segments
rfm.describe().T

rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])

# 2. With the help of RFM analysis, find the customers in the relevant profile for 2 cases and save the customer IDs to CSV.
# a. FLO is adding a new women's shoe brand. The product prices of the included brand are above general customer preferences. Therefore the brand
#We want to be able to specifically contact customers with the profile that would be interested in # promotion and product sales. From your loyal customers(champions,loyal_customers),
# Customers who shop above 250 TL on average and from the women's category will be contacted privately. Enter the ID numbers of these customers into the csv file.
# Save as new_brand_target_customer_id.cvs.
#b. Nearly 40% discount is planned for Men's and Children's products. Those who have been good customers in the past but have been for a long time are interested in categories related to this discount.
# Customers who do not shop and should not be lost, those who are asleep and new customers are specifically targeted. Save the IDs of customers in the appropriate profile to the csv file discount_target_customer_ids.csv
#Save it as #.

print(rfm.columns)

target_segment_cust_ids = rfm[rfm["segment"].isin(["champions", "loyal_customers"])]["customer_id"]
cust_ids = \
df[(df["master_id"].isin(target_segment_cust_ids)) & (df["interested_in_categories_12"].str.contains("KADIN"))][
    "master_id"]

cust_ids.to_csv("yeni_ayakkabı_hedef_müşteri_id.csv", index=False)


#Functionalization of the entire process

def create_rfm(dataframe):
    # veriyi hazırlama
    dataframe["total_order_num"] = dataframe["order_num_total_ever_offline"] + dataframe["order_num_total_ever_online"]
    dataframe["total_value_online"] = dataframe["customer_value_total_ever_online"] + dataframe[
        "customer_value_total_ever_online"]
    dataframe["total_value_offline"] = dataframe["customer_value_total_ever_offline"] + dataframe[
        "customer_value_total_ever_offline"]
    dataframe["total_value"] = dataframe["customer_value_total_ever_offline"] + dataframe[
        "customer_value_total_ever_online"]

    date_columns = df.columns[df.columns.str.contains("date")]
    df[date_columns] = df[date_columns].apply(pd.to.datetime)

    # calculation of rfm metrics

    analysis_date = dt.datetime(2021, 6, 1)
    rfm = pd.DataFrame()
    rfm["customer_id"] = dataframe["master_id"]
    rfm["recency"] = (analysis_date - dataframe["last_order_date"]).astype("timedelta64[D]")
    rfm["frequency"] = dataframe["total_order_num"]
    rfm["monetary"] = dataframe["total_value"]

# Calculation of Scores

    rfm["recency_score"] = pd.qcut(rfm["recency"], 5, [5, 4, 3, 2, 1])
    rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, [1, 2, 3, 4, 5])
    rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, [1, 2, 3, 4, 5])
    rfm["rfm_score"] = rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str)

   # segmentation

    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'}

    rfm["segment"] = rfm["rfm_score"].replace(seg_map, regex=True)

    return rfm[["customer_id", "recency", "frequency", "monetary", "rfm_score", "segment"]]


refm_df = create_rfm(df)
