import pandas as pd

def extract_data():
    """
    Extract data from CSV files into separate DataFrames.

    :return: Tuple of client DataFrame, campaign DataFrame, and economics DataFrame
    """
    try:
        # Read the CSV files and store them in DataFrames
        client_df = pd.read_csv('data/client.csv')
        campaign_df = pd.read_csv('data/campaign.csv')
        economics_df = pd.read_csv('data/economics.csv')

    # Handle specific exceptions
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return None, None, None

    except pd.errors.EmptyDataError as e:
        print(f"Error: {e}")
        return None, None, None

    # Handle any other exceptions
    except Exception as e:
        print(f"Error: {e}")
        return None, None, None

    return client_df, campaign_df, economics_df

# Test the function
client_df, campaign_df, economics_df = extract_data()

#%%
