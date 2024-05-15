# prefect_pipeline.py
import prefect
from prefect import task, Flow
import pandas as pd
import psycopg2
import yaml

# Load configuration from config.yaml
with open('../config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)


# Define tasks
@task
def extract_task():
    """
    Extract data from CSV files into separate DataFrames.

    :return: Tuple of client DataFrame, campaign DataFrame, and economics DataFrame
    """
    try:
        # Read the CSV files and store them in DataFrames
        client_df = pd.read_csv(config_data['client_filepath'])
        campaign_df = pd.read_csv(config_data['campaign_filepath'])
        economics_df = pd.read_csv(config_data['economics_filepath'])
    except FileNotFoundError as e:
        raise Exception(f"Error: File not found. {e}")
    except pd.errors.EmptyDataError as e:
        raise Exception(f"Error: Empty data encountered. {e}")
    except Exception as e:
        raise Exception(f"Error during data extraction: {e}")

    return client_df, campaign_df, economics_df


@task
def transform_task(client_df, campaign_df, economics_df):
    # Remove duplicate rows
    client_df = client_df.drop_duplicates()
    campaign_df = campaign_df.drop_duplicates()
    economics_df = economics_df.drop_duplicates()

    # Handle missing values
    client_df = client_df.fillna(0)
    campaign_df = campaign_df.fillna(0)
    economics_df = economics_df.fillna(0)

    # Convert column data types
    client_df['age'] = client_df['age'].astype(int)
    campaign_df['number_contacts'] = campaign_df['number_contacts'].astype(int)
    economics_df['cons_price_idx'] = economics_df['cons_price_idx'].astype(float)

    # Merge DataFrames
    bank_marketing = pd.merge(client_df, campaign_df, on='client_id', how='inner')
    bank_marketing = pd.merge(bank_marketing, economics_df, on='client_id', how='inner')

    return bank_marketing


@task
def load_task(bank_marketing):
    """
    Loads data into a PostgreSQL database using connection pooling.
    """

    # Connect to database using connection pooling
    conn = psycopg2.connect(**config_data['postgresql'])  # Unpack dictionary for connection params
    cursor = conn.cursor()

    try:
        # Create tables (if they don't exist)
        for table_sql in [config_data['client_create_PSQL'], config_data['campaign_create_PSQL'], config_data['economics_create_PSQL']]:
            cursor.execute(table_sql)
        conn.commit()

        # Load data into tables
        for table, data in [('client', bank_marketing[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']]),
                            ('campaign', bank_marketing[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'last_contact_date']]),
                            ('economics', bank_marketing[['client_id', 'cons_price_idx', 'euribor_three_months']])]:
            cursor.executemany(config_data[f"{table}_insert_PSQL"], data.itertuples(index=False))

        conn.commit()

    except Exception as e:
        raise Exception(f"Error during data loading: {e}")

    finally:
        # Close cursor and connection
        cursor.close()
        conn.close()


# Define the Prefect flow with logging
with Flow("Bank Marketing ETL") as flow:
    extract_data = extract_task()
    transformed_data = transform_task(*extract_data)
    load_task(transformed_data)

# This ensures that Prefect registers the flow
flow.register(project_name="Bank Marketing ETL")  # Replace "Bank Marketing ETL" with your project name

# Run the flow
if __name__ == "__main__":
    flow.run()
