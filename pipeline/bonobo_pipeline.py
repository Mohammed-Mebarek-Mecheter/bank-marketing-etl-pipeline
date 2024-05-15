# Import Modules
import bonobo
from collections.abc import Iterable

# Import ETL Activities
from etl import extract, transform, load

# Import Configuration
import yaml

# Step 1: Extract data
client_df, campaign_df, economics_df = extract.extract_data()

# Step 2: Transform Data
merged_df = transform.clean_data(client_df, campaign_df, economics_df)

# Step 3: Load Data
with open('config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)

# Define the Bonobo pipeline
def get_graph(**kwargs):
    graph = bonobo.Graph()
    graph.add_chain(
        extract.extract_data,
        transform.clean_data,
        load.load_data,
        _input=config_data
    )
    return graph

# Define the main function to run the Bonobo pipeline
def main():
    # Set the options for the Bonobo pipeline
    services = bonobo.get_services_from_parser()

    # Run the Bonobo pipeline
    bonobo.run(get_graph(), services=services)

if __name__ == '__main__':
    main()