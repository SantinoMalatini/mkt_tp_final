import os

from etl.extract.extract import extract_raw_tables

from etl.transform.build_dim_customer import build_dim_customer
from etl.transform.build_dim_channel import build_dim_channel
from etl.transform.build_dim_location import build_dim_location
from etl.transform.build_dim_product import build_dim_product
from etl.transform.build_dim_store import build_dim_store
from etl.transform.build_dim_date import build_dim_date

from etl.transform.build_fact_sales import build_fact_sales
from etl.transform.build_fact_payments import build_fact_payments
from etl.transform.build_fact_shipments import build_fact_shipments
from etl.transform.build_fact_web_sessions import build_fact_web_sessions
from etl.transform.build_nps_responses import build_fact_nps_responses


def pipeline(dim_output_path, fact_output_path):
   
    data = extract_raw_tables()

    df_dim_customer = build_dim_customer(data)
    df_dim_channel = build_dim_channel(data)
    df_dim_location = build_dim_location(data)
    df_dim_product = build_dim_product(data)
    df_dim_store = build_dim_store(data)
    df_dim_date = build_dim_date()
    
    df_fact_sales = build_fact_sales(data, df_dim_date, df_dim_customer, df_dim_product, df_dim_channel, df_dim_store, df_dim_location)
    df_fact_payments = build_fact_payments(data, df_dim_date, df_dim_customer, df_dim_channel)
    df_fact_shipment = build_fact_shipments(data, df_dim_date, df_dim_customer, df_dim_location)
    df_fact_web_sessions = build_fact_web_sessions(data, df_dim_date, df_dim_customer)
    df_fact_nps_responses = build_fact_nps_responses(data, df_dim_date, df_dim_customer, df_dim_channel)


    dataframes = [
        ("dim_customer", df_dim_customer),
        ("dim_channel", df_dim_channel),
        ("dim_location", df_dim_location),
        ("dim_product", df_dim_product),
        ("dim_store", df_dim_store),
        ("dim_date", df_dim_date),
        ("fact_sales", df_fact_sales),
        ("fact_payments", df_fact_payments),
        ("fact_shipments", df_fact_shipment),
        ("fact_web_sessions", df_fact_web_sessions),
        ("fact_nps_responses", df_fact_nps_responses)
    ]

    for name, df in dataframes:
        name_lower = name.lower()

        if "fact" in name_lower:
            output_dir = fact_output_path
        elif "dim" in name_lower:
            output_dir = dim_output_path

        filepath = os.path.join(output_dir, f"{name}.csv")
        df.to_csv(filepath, index=False)
        print(f"{name} guardado en: {filepath}")
