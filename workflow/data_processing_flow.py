from prefect import task, flow
from prefect.schedules import IntervalSchedule

# Import functions from your existing ETL scripts
from marketing.etl import extract, transform, load

# Tasks for data extraction, transformation, and loading
@task
def extract_data_task():
    """Extracts data from CSV files."""
    return extract()

@task
def transform_data_task(extracted_data):
    """Transforms and cleans the extracted data."""
    return transform(*extracted_data)  # Unpack the tuple from extract_data

@task
def load_data_task(transformed_data):
    """Loads the transformed data into the database."""
    load(transformed_data)

# Define the data processing flow
@flow(schedule=IntervalSchedule(minutes=1))  # Schedule to run every minute (adjust as needed)
def data_processing_flow():
    """
    This flow orchestrates the data extraction, transformation, and loading process.
    """
    extracted_data = extract_data_task()
    transformed_data = transform_data_task(extracted_data)
    load_data_task(transformed_data)

# Run the flow (optional for local execution)
data_processing_flow()
