import pandas as pd

def build_dim_date(data: dict):

    # === Unir todas las fechas relevantes de todas las tablas ===
    all_dates = pd.concat([
        pd.to_datetime(data['sales_order']['order_date'], errors='coerce'),
        pd.to_datetime(data['web_session']['started_at'], errors='coerce'),
        pd.to_datetime(data['nps_response']['responded_at'], errors='coerce'),
        pd.to_datetime(data['payment']['paid_at'], errors='coerce'),
        pd.to_datetime(data['shipment']['shipped_at'], errors='coerce'),
        pd.to_datetime(data['shipment']['delivered_at'], errors='coerce'),
        pd.to_datetime(data['customer']['created_at'], errors='coerce'),
        pd.to_datetime(data['address']['created_at'], errors='coerce'),
        pd.to_datetime(data['product']['created_at'], errors='coerce'),
    ]).dropna()

    all_dates = all_dates.dt.normalize()

    if all_dates.empty:
        raise ValueError("No se encontraron fechas válidas en los DataFrames proporcionados.")

    # === Determinar fechas mínima y máxima ===
    start_date = all_dates.min()
    end_date = all_dates.max()

    # === Generar rango de fechas ===
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    df = pd.DataFrame({'full_date': date_range})

    # === Crear columnas de descomposición de fecha (misma estructura que la original) ===
    df['date_id'] = df['full_date'].dt.strftime('%Y%m%d').astype(int)
    df['day'] = df['full_date'].dt.day
    df['month'] = df['full_date'].dt.month
    df['month_name'] = df['full_date'].dt.month_name()
    df['quarter'] = df['full_date'].dt.quarter
    df['year'] = df['full_date'].dt.year
    df['day_of_week'] = df['full_date'].dt.day_name()
    df['is_weekend'] = df['day_of_week'].isin(['Saturday', 'Sunday'])

    # === Ordenar columnas ===
    dim_date = df[[
        'date_id',
        'full_date',
        'day',
        'month',
        'month_name',
        'quarter',
        'year',
        'day_of_week',
        'is_weekend'
    ]]

    print(f"Tabla DIM_DATE creada correctamente. Rango: {start_date.date()} → {end_date.date()} | Filas: {len(dim_date)}")

    return dim_date
