import psycopg2
import luigi
import pandas as pd
import yaml
from sqlalchemy import create_engine


# Load configuration from config.yaml
with open('config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)

class LoadDataTask(luigi.Task):
    def requires(self):
        return TransformData()

    def run(self):
        merged_df = pd.read_csv(self.input().path)

        # Split data into separate DataFrames
        client_df = merged_df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']]
        campaign_df = merged_df[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts',
                                 'previous_outcome', 'campaign_outcome', 'last_contact_date']]
        economics_df = merged_df[['client_id', 'cons_price_idx', 'euribor_three_months']]

        # Create a connection engine
        engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
            config_data['postgresql']['username'],
            config_data['postgresql']['password'],
            config_data['postgresql']['host'],
            config_data['postgresql']['port'],
            config_data['postgresql']['database_name']
        ))

        # Load data into PostgreSQL tables
        client_df.to_sql(config_data['client_table_PSQL'], engine, if_exists='append', index=False)
        campaign_df.to_sql(config_data['campaign_table_PSQL'], engine, if_exists='append', index=False)
        economics_df.to_sql(config_data['economics_table_PSQL'], engine, if_exists='append', index=False)


class ExtractData(luigi.Task):
    def output(self):
        return {
            'client': luigi.LocalTarget('data/client.csv'),
            'campaign': luigi.LocalTarget('data/campaign.csv'),
            'economics': luigi.LocalTarget('data/economics.csv')
        }

    def run(self):
        # Load data from sources
        client_df = pd.read_csv(config_data['client_filepath'])
        campaign_df = pd.read_csv(config_data['campaign_filepath'])
        economics_df = pd.read_csv(config_data['economics_filepath'])

        # Save data as targets
        client_df.to_csv(self.output()['client'].path, index=False)
        campaign_df.to_csv(self.output()['campaign'].path, index=False)
        economics_df.to_csv(self.output()['economics'].path, index=False)


class TransformData(luigi.Task):
    def requires(self):
        return ExtractData()

    def output(self):
        return luigi.LocalTarget('data/transformed.csv')

    def run(self):
        client_df = pd.read_csv(self.input()['client'].path)
        campaign_df = pd.read_csv(self.input()['campaign'].path)
        economics_df = pd.read_csv(self.input()['economics'].path)

        # Merge data
        merged_df = pd.merge(client_df, campaign_df, on='client_id', how='inner')
        merged_df = pd.merge(merged_df, economics_df, on='client_id', how='inner')

        # Clean data
        merged_df = merged_df.drop_duplicates()
        merged_df = merged_df.fillna(0)
        merged_df['age'] = merged_df['age'].astype(int)
        merged_df['number_contacts'] = merged_df['number_contacts'].astype(int)
        merged_df['cons_price_idx'] = merged_df['cons_price_idx'].astype(float)

        # Save transformed data
        merged_df.to_csv(self.output().path, index=False)


if __name__ == '__main__':
    luigi.run(['LoadDataTask', '--local-scheduler'])
