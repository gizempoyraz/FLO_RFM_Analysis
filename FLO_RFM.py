###############################################################
# RFM ile Müşteri Segmentasyonu (Customer Segmentation with RFM)
###############################################################

###############################################################
# İş Problemi (Business Problem)
###############################################################
# FLO müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama stratejileri belirlemek istiyor.
# Buna yönelik olarak müşterilerin davranışları tanımlanacak ve bu davranış öbeklenmelerine göre gruplar oluşturulacak..

###############################################################
# Veri Seti Hikayesi
###############################################################

# Veri seti son alışverişlerini 2020 - 2021 yıllarında OmniChannel(hem online hem offline alışveriş yapan) olarak yapan müşterilerin geçmiş alışveriş davranışlarından
# elde edilen bilgilerden oluşmaktadır.

# master_id: Eşsiz müşteri numarası
# order_channel : Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile, Offline)
# last_order_channel : En son alışverişin yapıldığı kanal
# first_order_date : Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date : Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online : Muşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline : Muşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online : Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline : Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline : Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online : Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12 : Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi


# GÖREV 1: Veriyi Anlama (Data Understanding) ve Hazırlama
# 1. flo_data_20K.csv verisini okuyunuz.
# 2. Veri setinde
# a. İlk 10 gözlem,
# b. Değişken isimleri,
# c. Betimsel istatistik,
# d. Boş değer,
# e. Değişken tipleri, incelemesi yapınız.
# 3. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Herbir müşterinin toplam
# alışveriş sayısı ve harcaması için yeni değişkenler oluşturun.
# 4. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
# 5. Alışveriş kanallarındaki müşteri sayısının, ortalama alınan ürün sayısının ve ortalama harcamaların dağılımına bakınız.
# 6. En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
# 7. En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
# 8. Veri ön hazırlık sürecini fonksiyonlaştırınız.

# 1. flo_data_20K.csv verisini okuyunuz.
import pandas as pd
import datetime as dt

df_ = pd.read_csv("datasets\flo_data_20k.csv")
df = df_.copy()

# a. İlk 10 gözlem,
df.head(10)

# b. Değişken isimleri
df.columns

# c. Betimsel istatistik
df.describe().T

# d. Boş değer
df.isnull().sum()
df.isnull().sum().any()

# e. Değişken tipleri, incelemesi yapınız.
df.info()

# 3. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Herbir müşterinin toplam
# alışveriş sayısı ve harcaması için yeni değişkenler oluşturun.
df["total_order_num"] = df["order_num_total_ever_offline"] + df["order_num_total_ever_online"]
df["total_value_online"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_online"]
df["total_value_offline"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_offline"]
df["total_value"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]
df.info()

# 4. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
date_columns = df.columns[df.columns.str.contains("date")]
df[date_columns] = df[date_columns].apply(pd.to_datetime)

# 5. Alışveriş kanallarındaki müşteri sayısının, ortalama alınan ürün sayısının ve toplam harcamaların dağılımına bakınız.
df.groupby("order_channel").agg({"master_id": "count",
                                 "total_order_num": "sum",
                                 "total_value": "sum"})

# 6. En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
df.sort_values("total_value", ascending=False).head(10)
# df.sort_values("total_value",ascending=False)[:10]

# 7. En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
df.sort_values("total_order_num", ascending=False).head(10)


# df.sort_values("total_order_num", ascending=False)[:10]

# 8. Veri ön hazırlık sürecini fonksiyonlaştırınız.

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


# GÖREV 2: RFM Metriklerinin Hesaplanması

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

# GÖREV 3: RF ve RFM Skorlarının Hesaplanması

rfm["recency_score"] = pd.qcut(rfm["recency"], 5, [5, 4, 3, 2, 1])
rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, [1, 2, 3, 4, 5])
rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, [1, 2, 3, 4, 5])

rfm.head()

rfm["rfm_score"] = rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str)

# GÖREV 4: RF Skorlarının Segment Olarak Tanımlanması

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


rfm["segment"] = rfm["rfm_score"].replace(seg_map, regex=True)
rfm["segment"] = rfm["rfm_score"].apply(lambda x: map_segment(x) if pd.notnull(x) else x)
rfm["segment"] = rfm["rfm_score"].apply(lambda x: map_segment(x))

rfm[rfm["rfm_score"] == "31"].head()
rfm[rfm["rfm_score"] == "55"][:12]

# GÖREV 5: Aksiyon zamanı!


# 1. Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.
rfm.describe().T

rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])

# 2. RFM analizi yardımı ile 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv ye kaydediniz.
# a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri tercihlerinin üstünde. Bu nedenle markanın
# tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak iletişime geçeilmek isteniliyor. Sadık müşterilerinden(champions,loyal_customers),
# ortalama 250 TL üzeri ve kadın kategorisinden alışveriş yapan kişiler özel olarak iletişim kuralacak müşteriler. Bu müşterilerin id numaralarını csv dosyasına
# yeni_marka_hedef_müşteri_id.cvs olarak kaydediniz.
# b. Erkek ve Çoçuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte iyi müşteri olan ama uzun süredir
# alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve yeni gelen müşteriler özel olarak hedef alınmak isteniliyor. Uygun profildeki müşterilerin id'lerini csv dosyasına indirim_hedef_müşteri_ids.csv
# olarak kaydediniz.

print(rfm.columns)

target_segment_cust_ids = rfm[rfm["segment"].isin(["champions", "loyal_customers"])]["customer_id"]
cust_ids = \
df[(df["master_id"].isin(target_segment_cust_ids)) & (df["interested_in_categories_12"].str.contains("KADIN"))][
    "master_id"]

cust_ids.to_csv("yeni_ayakkabı_hedef_müşteri_id.csv", index=False)


# GÖREV 6: Tüm süreci fonksiyonlaştırınız.

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

    # rfm metriklerinin hesaplanması

    analysis_date = dt.datetime(2021, 6, 1)
    rfm = pd.DataFrame()
    rfm["customer_id"] = dataframe["master_id"]
    rfm["recency"] = (analysis_date - dataframe["last_order_date"]).astype("timedelta64[D]")
    rfm["frequency"] = dataframe["total_order_num"]
    rfm["monetary"] = dataframe["total_value"]

    # Skorlarının Hesaplanması

    rfm["recency_score"] = pd.qcut(rfm["recency"], 5, [5, 4, 3, 2, 1])
    rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, [1, 2, 3, 4, 5])
    rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, [1, 2, 3, 4, 5])
    rfm["rfm_score"] = rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str)

    # segmentleme

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
