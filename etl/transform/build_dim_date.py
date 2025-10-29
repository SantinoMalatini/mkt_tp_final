import pandas as pd

def build_dim_date(start_date='2023-01-01', end_date='2025-12-31'):
    """
    Construye la tabla DIM_DATE con todas las fechas entre start_date y end_date.
    """
    # === Convertir a datetime para evitar errores ===
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # === Generar rango de fechas ===
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    df = pd.DataFrame({'full_date': date_range})

    # === Crear columnas de descomposición de fecha ===
    df['date_id'] = df['full_date'].dt.strftime('%Y%m%d').astype(int)
    df['day'] = df['full_date'].dt.day
    df['month'] = df['full_date'].dt.month
    df['month_name'] = df['full_date'].dt.month_name()
    df['quarter'] = df['full_date'].dt.quarter
    df['year'] = df['full_date'].dt.year
    df['day_of_week'] = df['full_date'].dt.day_name()
    df['is_weekend'] = df['day_of_week'].isin(['Saturday', 'Sunday'])

    # === Orden de columnas ===
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

    print(f"Tabla DIM_DATE creada correctamente. Filas: {len(dim_date)}")

    return dim_date
