import pandas as pd
import numpy as np

def build_dim_product(data):
    # === Extracción de DataFrames ===
    try:
        df_product = data['product']
        df_category = data['product_category']
    except KeyError as e:
        print(f"Error: Falta la tabla {e} en el diccionario 'data'.")
        return None

    # === Merge product + category ===
    dim_product = pd.merge(
        df_product,
        df_category[['category_id', 'name', 'parent_id']],
        on='category_id',
        how='left',
        suffixes=('', '_category')
    )

    # === Crear surrogate key ===
    dim_product = dim_product.reset_index(drop=True)
    dim_product['product_key'] = dim_product.index + 1

    # === Selección y renombrado de columnas finales ===
    dim_product_final = dim_product[[
        'product_key',
        'product_id',
        'sku',
        'name',
        'list_price',
        'status',
        'name_category',
        'parent_id'
    ]].rename(columns={
        'name': 'name',
        'name_category': 'category_name',
        'parent_id': 'category_parent_id'
    })

    # === Limpieza de duplicados y nulos ===
    dim_product_final = dim_product_final.drop_duplicates(subset=['product_id'])
    dim_product_final['status'] = dim_product_final['status'].fillna('UNKNOWN')
    dim_product_final['category_name'] = dim_product_final['category_name'].fillna('UNKNOWN')

    print(f"Tabla DIM_PRODUCT creada correctamente. Filas: {len(dim_product_final)}")

    return dim_product_final
