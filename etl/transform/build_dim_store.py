import pandas as pd

def build_dim_store(data):
    # === Extracción de DataFrames ===
    try:
        df_store = data['store']
        df_address = data['address']
        df_province = data['province']
    except KeyError as e:
        print(f"Error: Falta la tabla {e} en el diccionario 'data'.")
        return None

    # === Merge store + address + province ===
    dim_store = pd.merge(df_store, df_address, on='address_id', how='left', suffixes=('_store', '_address'))
    dim_store = pd.merge(dim_store, df_province, on='province_id', how='left', suffixes=('', '_province'))

    # === Crear surrogate key (la llamamos 'id' como solicitaste) ===
    dim_store = dim_store.reset_index(drop=True)
    dim_store['store_id'] = dim_store.index + 1 

    # === Renombrar columnas para consistencia ===
    if 'name' in dim_store.columns:
        dim_store = dim_store.rename(columns={'name': 'name_store'})

    # === Selección y renombrado de columnas finales ===
    dim_store_final = dim_store[[
        'store_id', 
        'name_store',
        'line1',
        'city',
        'postal_code',
        'name_province'
    ]].rename(columns={
        'name_store': 'name',
        'line1': 'address_line1',
        'city': 'address_city',
        'postal_code': 'address_postal_code',
        'name_province': 'address_province_name'
    })

    # === Limpieza de nulos ===
    dim_store_final = dim_store_final.fillna({
        'address_line1': 'UNKNOWN',
        'address_city': 'UNKNOWN',
        'address_postal_code': 'UNK',
        'address_province_name': 'UNKNOWN'
    })

    print(f"Tabla DIM_STORE creada correctamente. Filas: {len(dim_store_final)}")

    return dim_store_final