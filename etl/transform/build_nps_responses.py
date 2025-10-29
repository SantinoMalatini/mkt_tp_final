import pandas as pd
from typing import Dict, Optional

def build_fact_nps_responses(data: Dict[str, pd.DataFrame], 
                             dim_date: pd.DataFrame, 
                             dim_customer: pd.DataFrame, 
                             dim_channel: pd.DataFrame) -> Optional[pd.DataFrame]:

    # === 1. Extracción de DataFrame Transaccional ===
    df_nps = data.get('nps_response')
    if df_nps is None:
        return None

    fact_nps = df_nps.copy()

    # === 2. Join con dim_date para obtener responded_date_id ===
    fact_nps['responded_date'] = pd.to_datetime(fact_nps['responded_at']).dt.date
    date_map = dim_date[['date_id', 'full_date']].copy()
    date_map['full_date'] = pd.to_datetime(date_map['full_date']).dt.date

    fact_nps = fact_nps.merge(date_map.rename(columns={'date_id': 'responded_date_id', 'full_date': 'responded_date'}),
                              on='responded_date', how='left')
    fact_nps.drop(columns=['responded_date'], inplace=True)

    # === 3. Joins con dimensiones Customer y Channel ===
    fact_nps = fact_nps.merge(dim_customer[['customer_id']], on='customer_id', how='left')
    fact_nps = fact_nps.merge(dim_channel[['channel_id']], on='channel_id', how='left')

    # === 4. Crear Surrogate Key ===
    fact_nps = fact_nps.reset_index(drop=True)
    fact_nps['nps_responses_id'] = fact_nps.index + 1

    # === 5. Selección y renombrado de columnas finales ===
    fact_nps.rename(columns={'nps_id': 'nps_response_id'}, inplace=True)
    
    final_columns = [
        'nps_responses_id', 
        'responded_date_id',
        'customer_id',      
        'channel_id',      
        'nps_response_id',  
        'comment',          
        'score'               
    ]
    fact_nps = fact_nps.reindex(columns=final_columns)

    # === 6. Limpieza de nulos ===
    fact_nps[['responded_date_id', 'customer_id', 'channel_id']] = fact_nps[['responded_date_id', 'customer_id', 'channel_id']].fillna(0).astype(int)
    fact_nps['score'] = fact_nps['score'].fillna(0)
    fact_nps['comment'] = fact_nps['comment'].fillna('UNKNOWN')
    fact_nps['nps_response_id'] = fact_nps['nps_response_id'].fillna(0).astype(int)

    return fact_nps
