# workflow/prefect_workflow.py

from prefect import flow, task
from etl.extract import extract_data
from etl.transform import clean_data
from etl.load import load_data


@task
def extract_task():
    return extract_data()

@task
def transform_task(client_df, campaign_df, economics_df):
    return clean_data(client_df, campaign_df, economics_df)

@task
def load_task(bank_marketing):
    load_data(bank_marketing)

@flow(name="Bank Marketing ETL")
def etl_pipeline():
    client_df, campaign_df, economics_df = extract_task()
    bank_marketing = transform_task(client_df, campaign_df, economics_df)
    load_task(bank_marketing)

if __name__ == "__main__":
    etl_pipeline.serve(name="bank-marketing-etl",
                       tags=["etl", "bank-marketing"],
                       interval=3600)  # Run once every hour