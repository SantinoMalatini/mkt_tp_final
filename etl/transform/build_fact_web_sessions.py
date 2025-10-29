import pandas as pd
from typing import Dict, Optional

def build_fact_web_sessions(data: Dict[str, pd.DataFrame], 
                            dim_date: pd.DataFrame, 
                            dim_customer: pd.DataFrame) -> Optional[pd.DataFrame]:

    df_sessions = data.get('web_session')
    if df_sessions is None:
        return None

    fact_sessions = df_sessions.copy()

    # === Join con dim_date ===
    fact_sessions['session_date'] = pd.to_datetime(fact_sessions['started_at']).dt.date
    date_map = dim_date[['date_id', 'full_date']].copy()
    date_map['full_date'] = pd.to_datetime(date_map['full_date']).dt.date

    fact_sessions = fact_sessions.merge(
        date_map.rename(columns={'date_id': 'session_start_date_id', 'full_date': 'session_date'}),
        on='session_date',
        how='left'
    )
    fact_sessions.drop(columns=['session_date'], inplace=True)

    # === Join con dim_customer ===
    fact_sessions = fact_sessions.merge(dim_customer[['customer_id']], on='customer_id', how='left')

    # === Crear Surrogate Key ===
    fact_sessions = fact_sessions.reset_index(drop=True)
    fact_sessions['web_session_id'] = fact_sessions.index + 1

    # === Selección de columnas finales ===
    final_columns = ['web_session_id', 'session_start_date_id', 'customer_id']
    degen_cols = [col for col in ['source', 'device'] if col in fact_sessions.columns]
    final_columns.extend(degen_cols)

    fact_sessions = fact_sessions.reindex(columns=final_columns)

    # === Limpieza de nulos ===
    fact_sessions[['session_start_date_id', 'customer_id']] = fact_sessions[['session_start_date_id', 'customer_id']].fillna(0).astype(int)
    for col in degen_cols:
        fact_sessions[col] = fact_sessions[col].fillna('UNKNOWN')

    return fact_sessions
