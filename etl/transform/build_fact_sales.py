import pandas as pd
from typing import Dict, Optional

def build_fact_sales(data: Dict[str, pd.DataFrame], 
                     dim_date: pd.DataFrame, 
                     dim_customer: pd.DataFrame, 
                     dim_product: pd.DataFrame, 
                     dim_channel: pd.DataFrame, 
                     dim_store: pd.DataFrame, 
                     dim_location: pd.DataFrame) -> Optional[pd.DataFrame]:

    df_order = data.get('sales_order')
    df_item = data.get('sales_order_item')
    if df_order is None or df_item is None:
        return None

    # === 1. Crear tabla base ===
    fact_sales = df_order.merge(df_item, on='order_id', how='inner')

    # === 2. Join con dim_date ===
    date_map = dim_date[['date_id', 'full_date']].copy()
    date_map['full_date'] = pd.to_datetime(date_map['full_date']).dt.date

    fact_sales['order_date_clean'] = pd.to_datetime(fact_sales['order_date']).dt.date
    fact_sales = fact_sales.merge(date_map.rename(columns={'date_id': 'order_date_id', 'full_date': 'order_date_clean'}),
                                  on='order_date_clean', how='left')
    fact_sales.drop(columns=['order_date_clean'], inplace=True)

    # === 3. Joins con dimensiones estándar ===
    dims = {
        'customer_id': dim_customer,
        'product_id': dim_product,
        'channel_id': dim_channel,
        'store_id': dim_store
    }
    for col, df_dim in dims.items():
        fact_sales = fact_sales.merge(df_dim[[col]], on=col, how='left')

    # === 4. Joins con dim_location ===
    location_map = dim_location[['location_id', 'address_id']]

    fact_sales = fact_sales.merge(location_map, left_on='billing_address_id', right_on='address_id', how='left')\
                           .rename(columns={'location_id': 'billing_location_id'}).drop(columns=['address_id'])
    fact_sales = fact_sales.merge(location_map, left_on='shipping_address_id', right_on='address_id', how='left')\
                           .rename(columns={'location_id': 'shipment_location_id'}).drop(columns=['address_id'])

    # === 5. Crear surrogate key ===
    fact_sales = fact_sales.reset_index(drop=True)
    fact_sales['sales_id'] = fact_sales.index + 1
    fact_sales.rename(columns={'discount': 'discount_amount', 'status': 'order_status'}, inplace=True)

    # === 6. Selección de columnas finales ===
    final_columns = [
        'sales_id', 'order_date_id', 'customer_id', 'product_id', 'channel_id', 'store_id',
        'billing_location_id', 'shipment_location_id', 'order_id', 'order_item_id',
        'order_status', 'currency_code', 'quantity', 'unit_price', 'discount_amount', 'line_total'
    ]
    fact_sales = fact_sales.reindex(columns=final_columns)

    # === 7. Limpieza de nulos ===
    fk_cols = ['order_date_id', 'customer_id', 'product_id', 'channel_id', 'store_id', 'billing_location_id', 'shipment_location_id']
    fact_sales[fk_cols] = fact_sales[fk_cols].fillna(0).astype(int)

    measure_cols = ['quantity', 'unit_price', 'discount_amount', 'line_total']
    fact_sales[measure_cols] = fact_sales[measure_cols].fillna(0)

    degen_cols = ['order_status', 'currency_code']
    fact_sales[degen_cols] = fact_sales[degen_cols].fillna('UNKNOWN')

    return fact_sales
