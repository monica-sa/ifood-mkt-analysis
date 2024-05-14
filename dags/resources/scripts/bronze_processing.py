import pandas as pd
import numpy as np

def process_bronze_data():
    # input_df = pd.read_csv('dags/resources/data/source/ifood_df.csv')
    input_df = pd.read_csv('/opt/airflow/dags/resources/data/source/ifood_df.csv')

    input_df.columns = input_df.columns.str.replace(' ', '')
    input_df.columns = input_df.columns.str.replace('_', '')

    cleaned_df = input_df[input_df['MntRegularProds'] >= 0]

    cleaned_df.to_csv('/opt/airflow/dags/resources/data/bronze/ifood_df_bronze.csv', index=False)

if __name__ == "__main__":
    process_bronze_data()
