import pandas as pd
import numpy as np

def build_dim_customer(data):
    # === Extracción de DataFrames ===
    try:
        df_customer = data['customer']
    except KeyError as e:
        print(f"Error: Falta la tabla {e} en el diccionario 'data'.")
        return None

    # === Crear columna full_name ===
    df_customer['full_name'] = df_customer['first_name'].fillna('') + ' ' + df_customer['last_name'].fillna('')
    df_customer['full_name'] = df_customer['full_name'].str.strip()

    # === Seleccionar columnas finales ===
    dim_customer = df_customer[[
        'customer_id',
        'email',
        'first_name',
        'last_name',
        'full_name',
        'phone',
        'status',
        'created_at'
    ]].drop_duplicates(subset=['customer_id'])

    # === Ordenar y limpiar ===
    dim_customer = dim_customer.reset_index(drop=True)
    dim_customer['status'] = dim_customer['status'].fillna('UNKNOWN')

    print(f"Tabla DIM_CUSTOMER creada correctamente. Filas: {len(dim_customer)}")

    return dim_customer
