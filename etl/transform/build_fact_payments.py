import pandas as pd
from typing import Dict, Optional

def build_fact_payments(data: Dict[str, pd.DataFrame], 
                        dim_date: pd.DataFrame, 
                        dim_customer: pd.DataFrame, 
                        dim_channel: pd.DataFrame) -> Optional[pd.DataFrame]:

    df_payment = data.get('payment')
    df_order = data.get('sales_order')
    if df_payment is None or df_order is None:
        return None

    # === 1. Merge base con sales_order ===
    fact_base = df_payment.merge(df_order, on='order_id', how='left', suffixes=('_payment', '_order'))

    # === 2. Join con dim_date (role-playing) ===
    date_map = dim_date[['date_id', 'full_date']].copy()
    date_map['full_date'] = pd.to_datetime(date_map['full_date']).dt.date

    for src_col, new_col in [('paid_at', 'paid_date_id'), ('order_date', 'order_date_id')]:
        fact_base[src_col + '_clean'] = pd.to_datetime(fact_base[src_col]).dt.date
        fact_base = fact_base.merge(
            date_map.rename(columns={'date_id': new_col, 'full_date': src_col + '_clean'}),
            on=src_col + '_clean',
            how='left'
        )

    fact_base.drop(columns=['paid_at_clean', 'order_date_clean'], inplace=True)

    # === 3. Joins con dimensiones estándar ===
    fact_base = fact_base.merge(dim_customer[['customer_id']], on='customer_id', how='left')
    fact_base = fact_base.merge(dim_channel[['channel_id']], on='channel_id', how='left')

    # === 4. Crear surrogate key ===
    fact_base = fact_base.reset_index(drop=True)
    fact_base['payments_id'] = fact_base.index + 1

    # === 5. Renombrar columnas y seleccionar finales ===
    fact_base.rename(columns={'method': 'payment_method', 'status_payment': 'payment_status'}, inplace=True)

    final_columns = [
        'payments_id',
        'paid_date_id',
        'order_date_id',
        'customer_id',
        'channel_id',
        'order_id',
        'payment_method',
        'payment_status',
        'transaction_ref',
        'amount'
    ]
    fact_base = fact_base.reindex(columns=final_columns)

    # === 6. Limpieza de nulos ===
    fk_cols = ['paid_date_id', 'order_date_id', 'customer_id', 'channel_id']
    fact_base[fk_cols] = fact_base[fk_cols].fillna(0).astype(int)

    measure_cols = ['amount']
    fact_base[measure_cols] = fact_base[measure_cols].fillna(0)

    degen_cols = ['payment_method', 'payment_status', 'transaction_ref', 'order_id']
    fact_base[degen_cols] = fact_base[degen_cols].fillna('UNKNOWN')

    return fact_base
