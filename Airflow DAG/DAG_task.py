from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import time
import pymongo
import pandas as pd

#Define default arguments
default_args = {
 'owner': 'Muhammad Umair',
 'start_date': datetime (2024, 2, 19),
 'retries': 1,
}

# Instantiate your DAG
dag = DAG ('DAG_Test_Task', default_args=default_args, schedule_interval=timedelta(minutes=30))

# Set up logging
logging.basicConfig(filename='log_file.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_mongodb(uri="mongodb+srv://tut68901:_f!CDs6ZEMkWZ86@cluster0.2w6rfen.mongodb.net/?retryWrites=true&w=majority"):
    """Connects to the MongoDB database and returns the client and campaign_db.

    Args:
        uri (str, optional): The MongoDB connection URI. Defaults to the provided URI.

    Returns:
        tuple: A tuple containing the MongoDB client and the "campaign_db" database object.
    """

    client = pymongo.MongoClient(uri)
    db = client['campaign_db']
    return client, db


def fetch_data_from_mongodb(db, collection_name):
    """Fetches data from a MongoDB collection and returns a pandas DataFrame.

    Args:
        db (pymongo.database.Database): The MongoDB database object.
        collection_name (str): The name of the collection to fetch data from.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the fetched data.
    """

    table = db[collection_name]
    data_list = list(table.find())
    df = pd.DataFrame(data_list)
    return df

def clean_dataframe(df):
    """Cleans and prepares a DataFrame for further analysis.
       Also get last 24 hours of data.

    Args:
        df (pd.DataFrame): The DataFrame to clean.

    Returns:
        pd.DataFrame: The cleaned DataFrame with last 24 hours data points.
    """

    # Drop '_id' columns (if necessary)
    if '_id' in df.columns:
        df.drop(columns='_id', inplace=True)

    # Remove duplicates
    df.drop_duplicates(ignore_index=True, inplace=True)

    # Convert 'DateTime' to datetime objects
    df['DateTime'] = pd.to_datetime(df['DateTime'])

    # Round 'DateTime' to the nearest hour
    df['DateTime'] = df['DateTime'].dt.round('h')

    # Set DateTime as a index to apply last method 
    df.set_index('DateTime', inplace=True)

    # return last 24 hours data
    df = df.last('24h').reset_index()
    
    return df

def extract_relevant_columns(df, cols):
    """Extracts relevant columns from a DataFrame for each data source.

    Args:
        df (pd.DataFrame): The DataFrame to extract columns from.
        cols (list): A list of column names to extract from the data.

    Returns:
        DataFrame: Return dataframe with selected columns.
    """

    df = df[cols]
    
    return df

def aggregate_revenue_data(revenue_df, group_cols, agg_funcs):
    """Aggregates revenue data by specified groups and applies aggregation functions.

    Args:
        revenue_df (pd.DataFrame): The revenue DataFrame to aggregate.
        group_cols (list): A list of columns to group by.
        agg_funcs (dict): A dictionary mapping columns to aggregation functions.

    Returns:
        pd.DataFrame: The aggregated revenue DataFrame.
    """

    def revenue_count_val(x):
        x = len(revenue_df[revenue_df['DateTime']==x])
        return x
    
    revenue_df['count'] = revenue_df['DateTime'].apply(revenue_count_val)
    revenue_df = revenue_df[revenue_df['count']==2]
    revenue_df.drop(columns=['count'])

    return revenue_df.groupby(group_cols).agg(agg_funcs).reset_index()

def merge_dataframes(campaign_df, revenue_df, on_cols, suffixes):
    """Merges two DataFrames by specified columns and adds suffixes to avoid conflicts.

    Args:
        campaign_df (pd.DataFrame): The campaign DataFrame.
        revenue_df (pd.DataFrame): The revenue DataFrame.
        on_cols (list): A list of columns to merge on.
        suffixes (tuple): A tuple of suffixes to append to column names in each DataFrame.

    Returns:
        pd.DataFrame: The merged DataFrame.
    """

    return pd.merge(campaign_df, revenue_df, on=on_cols, how='inner', suffixes=suffixes)

def identify_discrepancies(merged_df):
    """Identifies rows with missing values in any column of the merged DataFrame.

    Args:
        merged_df (pd.DataFrame): The merged DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing rows with discrepancies.
    """

    return merged_df[merged_df.isnull().any(axis=1)]

def store_file(merged_data):
    """Identifies rows with missing values in any column of the merged DataFrame.

    Args:
        merged_df (pd.DataFrame): The merged DataFrame.

    """
    
    # Load data from the CSV file into a DataFrame
    file_path = 'merged_data.csv'
    file_data = pd.read_csv(file_path)
    file_data['DateTime'] = pd.to_datetime(file_data['DateTime'])

    # Merge the DataFrames on the datetime column
    final_merged_data = pd.merge(file_data, merged_data, on='DateTime', how='outer')

    # Update the data in the file with the data from the DataFrame where datetime matches
    file_data.update(final_merged_data, overwrite=True)

    # New rows 
    new_rows = merged_data[~merged_data['DateTime'].isin(file_data['DateTime'])]
    
    # Add new data in file
    file_data = file_data._append(new_rows, ignore_index=True)

    # Store merged DataFrame in a CSV file
    file_data.to_csv(file_path, index=False)
    print("Merged data successfully stored in merged_data.csv")

def main():
    try:
        # Connect to MongoDB
        client, db = connect_to_mongodb()

        # Fetch data from collections
        campaign_df = fetch_data_from_mongodb(db, "campaign")
        revenue_df = fetch_data_from_mongodb(db, "revenue")

        # Clean and prepare data
        campaign_df = clean_dataframe(campaign_df)
        revenue_df = clean_dataframe(revenue_df)

        # Extract relevant columns
        campaign_cols = ['DateTime', 'Ad Group', 'Impressions', 'Clicks', 'Cost', 'Conversions', 'Conv Rate', 'CPC']
        revenue_cols = ['DateTime', 'Ad Group', 'Impressions', 'Clicks', 'Conversions', 'Conv Rate', 'Revenue', 'Sale Amount']
        campaign_extracted = extract_relevant_columns(campaign_df, campaign_cols)
        revenue_extracted = extract_relevant_columns(revenue_df, revenue_cols)

        # Aggregate revenue data
        revenue_aggregated = aggregate_revenue_data(revenue_df, group_cols=['DateTime'],
                                                    agg_funcs={'Impressions': 'sum', 'Clicks': 'sum', 'Conversions': 'sum',
                                                            'Conv Rate': 'mean', 'Revenue': 'sum', 'Sale Amount': 'sum'})

        # Merge DataFrames
        merged_df = merge_dataframes(campaign_extracted, revenue_aggregated, on_cols=['DateTime'],
                                    suffixes=('_campaign', '_revenue'))
        print(merged_df)
        # Store data
        store_file(merged_df)
        
        # Log successful execution with timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"{timestamp} - Job executed successfully")
        
    except Exception as e:
        print(e)
        # Log the error with timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.error(f"{timestamp} - An error occurred: {e}")

        # Retry the job up to 3 times
        for _ in range(3):
            time.sleep(60)  # Wait for 1 minute before retrying
            try:
                main()
                break  # If successful, break out of the retry loop
            except Exception as retry_error:
                # Log the retry error with timestamp
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                logging.error(f"{timestamp} - Retry failed. Another error occurred: {retry_error}")
        else:
            # If all retries fail, log the failure with timestamp
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logging.error(f"{timestamp} - Job failed after 3 attempts. Check the logs for details.")


task_1 = PythonOperator(
 task_id='task_1',
 python_callable=main,
 dag=dag,
)

# Set task dependencies
task_1