from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv
from google.cloud import bigquery

# Load environment variables
dotenv_path = os.path.join(os.getcwd(), 'configs', '.env')
load_dotenv(dotenv_path)

# MongoDB & BigQuery configs
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")
MONGO_COLLECTION = os.getenv("MONGODB_COLLECTION")
BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")

print("DB:", MONGODB_DATABASE)

# Connect to MongoDB
client = MongoClient(MONGODB_URI)
db = client[MONGODB_DATABASE]
collection = db[MONGO_COLLECTION]

# Fetch data from MongoDB
df = pd.DataFrame(list(collection.find()))

# Flatten nested fields
df = pd.json_normalize(df.to_dict(orient='records'))

# Convert ObjectId to string
if '_id' in df.columns:
    df['_id'] = df['_id'].astype(str)

# Rename columns to be BigQuery-compatible (replace dots with underscores)
df.columns = [col.replace('.', '_') for col in df.columns]

# Initialize BigQuery client
bq_client = bigquery.Client(project=BIGQUERY_PROJECT)
table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

# Load DataFrame to BigQuery
job = bq_client.load_table_from_dataframe(
    df,
    table_id,
    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
)
job.result()  
print("Data successfully uploaded to BigQuery!")
