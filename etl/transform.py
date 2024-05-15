import pandas as pd
from etl.extract import extract_data

def clean_data(client_df, campaign_df, economics_df):
    # Remove duplicate rows
    client_df = client_df.drop_duplicates()
    campaign_df = campaign_df.drop_duplicates()
    economics_df = economics_df.drop_duplicates()

    # Handle missing values
    client_df = client_df.dropna()
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

if __name__ == '__main__':
    client_df, campaign_df, economics_df = extract_data()
    bank_marketing = clean_data(client_df, campaign_df, economics_df)
    print(bank_marketing.head())
#%%
