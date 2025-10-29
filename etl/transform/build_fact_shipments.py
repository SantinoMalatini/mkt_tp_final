import pandas as pd
from typing import Dict, Optional

def build_fact_shipments(data: Dict[str, pd.DataFrame],
                         dim_date: pd.DataFrame,
                         dim_customer: pd.DataFrame,
                         dim_location: pd.DataFrame) -> Optional[pd.DataFrame]:

    df_shipment = data.get('shipment')
    df_sales_order = data.get('sales_order')
    if df_shipment is None or df_sales_order is None:
        return None

    # === 1. Crear tabla base uniendo con sales_order ===
    order_details = df_sales_order[['order_id', 'customer_id', 'shipping_address_id', 'order_date']]
    fact_shipments = df_shipment.merge(order_details, on='order_id', how='left')

    # === 2. Join con dim_date (role-playing) ===
    date_map = dim_date[['date_id', 'full_date']].copy()
    date_map['full_date'] = pd.to_datetime(date_map['full_date']).dt.date

    for col, new_col in [('shipped_at', 'shipped_date_id'),
                         ('delivered_at', 'delivered_date_id'),
                         ('order_date', 'order_date_id')]:
        fact_shipments[col + '_clean'] = pd.to_datetime(fact_shipments[col], errors='coerce').dt.date
        fact_shipments = fact_shipments.merge(
            date_map.rename(columns={'date_id': new_col, 'full_date': col + '_clean'}),
            on=col + '_clean',
            how='left'
        )

    fact_shipments.drop(columns=['shipped_at_clean', 'delivered_at_clean', 'order_date_clean'], inplace=True)

    # === 3. Join con dim_customer y dim_location ===
    fact_shipments = fact_shipments.merge(dim_customer[['customer_id']], on='customer_id', how='left')
    fact_shipments = fact_shipments.rename(columns={'customer_id': 'costumer_id'})

    fact_shipments = fact_shipments.merge(
        dim_location[['location_id', 'address_id']],
        left_on='shipping_address_id',
        right_on='address_id',
        how='left'
    ).rename(columns={'location_id': 'shipment_location_id'})
    fact_shipments.drop(columns=['address_id'], inplace=True)

    # === 4. Crear surrogate key ===
    fact_shipments = fact_shipments.reset_index(drop=True)
    fact_shipments['shipments_id'] = fact_shipments.index + 1

    # === 5. Calcular medidas ===
    shipped_dt = pd.to_datetime(fact_shipments['shipped_at'], errors='coerce')
    delivered_dt = pd.to_datetime(fact_shipments['delivered_at'], errors='coerce')
    fact_shipments['delivery_time_days'] = (delivered_dt - shipped_dt).dt.days.fillna(0)

    fact_shipments.rename(columns={'status': 'shipment_status'}, inplace=True)

    # === 6. Seleccionar columnas finales ===
    final_columns = [
        'shipments_id',
        'shipped_date_id',
        'delivered_date_id',
        'order_date_id',
        'costumer_id',
        'shipment_location_id',
        'order_id',
        'carrier',
        'tracking_number',
        'shipment_status',
        'delivery_time_days'
    ]
    fact_shipments = fact_shipments.reindex(columns=final_columns)

    # === 7. Limpieza de nulos ===
    fk_cols = ['shipped_date_id', 'delivered_date_id', 'order_date_id', 'costumer_id', 'shipment_location_id']
    fact_shipments[fk_cols] = fact_shipments[fk_cols].fillna(0).astype(int)

    fact_shipments[['order_id', 'carrier', 'tracking_number', 'shipment_status']] = \
        fact_shipments[['order_id', 'carrier', 'tracking_number', 'shipment_status']].fillna('UNKNOWN')

    return fact_shipments
