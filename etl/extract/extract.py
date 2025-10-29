import os
import pandas as pd

def extract_raw_tables(raw_path="raw"):
    dataframes = {}

    for filename in os.listdir(raw_path):
        filepath = os.path.join(raw_path, filename)

        table_name, ext = os.path.splitext(filename)
        ext = ext.lower()

        try:
            df = pd.read_csv(filepath)
            dataframes[table_name] = df
            print(f"Cargado: {filename} → {table_name} ({len(df)} filas)")

        except Exception as e:
            print(f"Error al cargar {filename}: {e}")

    print("\nTablas cargadas:", list(dataframes.keys()))
    return dataframes

extract_raw_tables("raw")
