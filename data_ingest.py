import pymongo
import pandas as pd
from datetime import datetime, timedelta
import random
import schedule
import time
import configparser

# Read MongoDB credentials from config file
config = configparser.ConfigParser()
config.read('config.ini')

username = config['mongodb']['username']
password = config['mongodb']['password']

# Read data into a pandas DataFrame
df = pd.read_csv('./data/campaign_data.csv')

# Create MongoDB connection
client = pymongo.MongoClient(f"mongodb+srv://{username}:{password}@cluster0.2w6rfen.mongodb.net/?retryWrites=true&w=majority")
db = client["campaign_db"]

# Create Table 1 (Campaign)
table1_columns = ['Ad Group', 'Impressions', 'Clicks', 'Conversions', 'Conv Rate', 'Cost', 'CPC']
table1_data = df[table1_columns].copy()

# Create Table 2 (Revenue)
table2_columns = ['Ad Group', 'Impressions', 'Clicks', 'Conversions', 'Conv Rate', 'Revenue', 'Sale Amount']
table2_data = df[table2_columns].copy()

# Function to add noise to the revenue table
def add_noise(row):
    return row + random.uniform(-20, 20)

# Function to push data to MongoDB
def push_to_mongo(collection_name, df, date_column, table_type):
    df['DateTime'] = date_column
    df['Clicks'] = df['Clicks'].apply(add_noise)
    df['Conversions'] = df['Conversions'].apply(add_noise)
    df['Impressions'] = df['Impressions'].apply(add_noise)
    df['Conv Rate'] = df['Conversions'] / df['Clicks']

    # Add noise to the Revenue table
    if table_type == "Revenue":
        df['Sale Amount'] = df['Sale Amount'].apply(add_noise)
        df['Revenue'] = df['Revenue'].apply(add_noise)        
    else:
        df['Cost'] = df['Cost'].apply(add_noise)
        df['CPC'] = df['Cost'] / df['Clicks']

    # Convert df to list of dictionaries for MongoDB insertion
    records = df.to_dict(orient='records')

    # Insert records into MongoDB
    db[collection_name].insert_many(records)

    print(f"Data pushed to MongoDB collection: {collection_name} at {date_column}")

# Function to generate and push data every hour
def generate_and_push_campaign_data():
    # Create a timestamp for the current hour
    current_hour = datetime.now().replace(microsecond=0)

    # Add a data point to the Campaign table
    push_to_mongo('campaign', table1_data.sample(n=1, replace=True), current_hour, 'Campaign')

# Function to generate and push data every hour
def generate_and_push_revenue_data():
    # Create a timestamp for the current hour
    current_hour = datetime.now().replace(microsecond=0)
                                                
    # Add two data points to the Revenue table
    push_to_mongo('revenue', table2_data.sample(n=1, replace=True), current_hour, 'Revenue')


# Schedule the job to run every hour
schedule.every().hour.do(generate_and_push_campaign_data)

# Schedule the job to run every hour
schedule.every(29).to(30).minutes.do(generate_and_push_revenue_data)

# Run campaign function
generate_and_push_campaign_data()
generate_and_push_revenue_data()

# Run the scheduled job
while True:
    schedule.run_pending()
    time.sleep(1)
