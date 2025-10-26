import subprocess
import sys

def run_command(command, description):
    print(f"{description}...")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        print(f"Error during: {description}")
        sys.exit(1)

print("Starting Supply Chain Pipeline Setup")

# Check if dataset exists, else create
print("Checking if BigQuery dataset 'supply_chain' exists...")
check_dataset = subprocess.run("bq ls | grep -w supply_chain", shell=True)
if check_dataset.returncode != 0:
    run_command("bq --location=US mk supply_chain", "Creating BigQuery dataset 'supply_chain'")

# Run Mongo to BigQuery script
run_command("python mongo_to_bigquery/mongo_to_bigquery.py", "Running Mongo → BigQuery ingestion")

# Run dbt transformations
run_command("cd supply_chain_dbt && dbt run", "Executing dbt models")

print("Pipeline setup completed successfully!")
