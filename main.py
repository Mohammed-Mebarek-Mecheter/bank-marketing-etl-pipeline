from prefect import Flow
from workflow.prefect_pipeline import bank_marketing_etl

# Define the flow directly with the bank_marketing_etl function
flow = Flow("Bank Marketing ETL", tasks=[bank_marketing_etl])

# Run the flow
if __name__ == "__main__":
    flow.run()
