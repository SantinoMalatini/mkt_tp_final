import os
from etl.load.load import pipeline

dim_path = "warehouse/dim"
fact_path = "warehouse/fact"

def main():
    pipeline(dim_path, fact_path)
    
main()