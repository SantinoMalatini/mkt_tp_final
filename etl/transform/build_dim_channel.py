import pandas as pd

def build_dim_channel(data):
    # === Extracción de DataFrame ===
    try:
        df_channel = data['channel']
    except KeyError as e:
        print(f"Error: Falta la tabla {e} en el diccionario 'data'.")
        return None

    # === Selección de columnas ===
    dim_channel = df_channel[['channel_id', 'code', 'name']].drop_duplicates(subset=['channel_id'])

    # === Orden y limpieza ===
    dim_channel = dim_channel.reset_index(drop=True)
    dim_channel['name'] = dim_channel['name'].fillna('UNKNOWN')
    dim_channel['code'] = dim_channel['code'].fillna('UNK')

    print(f"Tabla DIM_CHANNEL creada correctamente. Filas: {len(dim_channel)}")

    return dim_channel
