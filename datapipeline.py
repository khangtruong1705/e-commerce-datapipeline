import pandas as pd
from sqlalchemy import create_engine
import unicodedata
import os
import json




# Connect DB
SOURCE_DB_URL = "postgresql://be_database_user:8ZB4313uq0T8vniaSJMsIoLw3aM5RqHu@dpg-d2lg9cndiees73c0qbrg-a.singapore-postgres.render.com/be_database"
TARGET_DB_URL = "postgresql://be_database_user:8ZB4313uq0T8vniaSJMsIoLw3aM5RqHu@dpg-d2lg9cndiees73c0qbrg-a.singapore-postgres.render.com/datawarehouse"

source_engine = create_engine(SOURCE_DB_URL)
target_engine = create_engine(TARGET_DB_URL)

# Categories
df_category_event = pd.read_sql("SELECT * FROM view_category_event", con=source_engine)
df_category_event['created_at'] = pd.to_datetime(df_category_event['created_at']).dt.date
df_category_event.to_sql("view_category_message", con=target_engine, if_exists="append", index=False)

# Products
def remove_diacritics(text):
    if pd.isnull(text): 
        return text
    return ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )

df_product_event = pd.read_sql("SELECT * FROM view_product_event", con=source_engine)
df_product_event['created_at'] = pd.to_datetime(df_product_event['created_at']).dt.date
df_product_event['name'] = df_product_event['name'].apply(remove_diacritics)
df_product_event.to_sql("view_product_message", con=target_engine, if_exists="append", index=False)

# Search
df_search_event = pd.read_sql("SELECT * FROM search_event", con=source_engine)
df_search_event['created_at'] = pd.to_datetime(df_search_event['created_at']).dt.date
df_search_event['keyword'] = df_search_event['keyword'].apply(remove_diacritics)
df_search_event.to_sql("search_message", con=target_engine, if_exists="append", index=False)


# Payment_table
df_payments = pd.read_sql("SELECT * FROM payments", con=source_engine)
df_payments.to_sql("payment_table", con=target_engine, if_exists="append", index=False)


# Order_items_table
df_order_items = pd.read_sql("SELECT * FROM order_items", con=source_engine)
df_order_items['items_list'] = df_order_items['items_list'].apply(json.dumps)
df_order_items.to_sql("order_items_table", con=target_engine, if_exists="append", index=False)


# shipping_table
df_shipping = pd.read_sql("SELECT phone,province FROM shipping", con=source_engine)
df_shipping.to_sql("shipping_table", con=target_engine, if_exists="append", index=False)

print("✅ Dữ liệu đã được import thành công!")
