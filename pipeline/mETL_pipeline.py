# mETL_pipeline.py

# Import required modules
from mETL import source, transform, target
import yaml

# Load configuration from config.yaml
with open('config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)

# Define data sources
client_source = source.csv(config_data['client_filepath'])
campaign_source = source.csv(config_data['campaign_filepath'])
economics_source = source.csv(config_data['economics_filepath'])

# Define transformations
merged_data = transform.join([client_source, campaign_source, economics_source], on='client_id')
cleaned_data = transform.drop_duplicates(merged_data)
cleaned_data = transform.fillna(cleaned_data, 0)
cleaned_data = transform.convert_type(cleaned_data, {'age': 'int', 'number_contacts': 'int', 'cons_price_idx': 'float'})

# Define data targets
client_target = target.csv('output/client.csv')
campaign_target = target.csv('output/campaign.csv')
economics_target = target.csv('output/economics.csv')

# Run the pipeline
mETL.source_to_target(cleaned_data, [client_target, campaign_target, economics_target])