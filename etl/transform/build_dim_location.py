import pandas as pd
import numpy as np

def build_dim_location(data):
    # === Extracción de DataFrames ===
    try:
        df_address = data['address']
        df_province = data['province']
    except KeyError as e:
        print(f"Error: Falta la tabla {e} en el diccionario 'data'.")
        return None

    # === Merge address + province ===
    dim_location = pd.merge(df_address, df_province, on='province_id', how='left')

    # === Crear surrogate key ===
    dim_location = dim_location.reset_index(drop=True)
    dim_location['location_id'] = dim_location.index + 1

    # === Selección y renombrado de columnas finales ===
    dim_location_final = dim_location[[
        'location_id',
        'address_id',
        'line1',
        'line2',
        'city',
        'postal_code',
        'country_code',
        'name',
        'code'
    ]].rename(columns={
        'name': 'province_name',
        'code': 'province_code'
    })

    # === Limpieza de duplicados y nulos ===
    dim_location_final = dim_location_final.drop_duplicates(subset=['line1', 'city', 'postal_code'])
    dim_location_final = dim_location_final.fillna({
        'line2': '',
        'city': 'UNKNOWN',
        'province_name': 'UNKNOWN',
        'province_code': 'UNK',
        'country_code': 'UNK'
    })

    print(f"Tabla DIM_LOCATION creada correctamente. Filas: {len(dim_location_final)}")

    return dim_location_final
