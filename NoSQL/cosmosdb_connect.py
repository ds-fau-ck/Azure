# Make sure to install below python library
# pip install azure-cosmos

import time
import uuid
from azure.cosmos import CosmosClient, exceptions, PartitionKey
import random

# Cosmos DB connection details
endpoint="*******************************"
key="************************************"
# Initialize the Cosmos client
client = CosmosClient(endpoint, key)

# Database and container details
database_name = 'DemoDatabase'
container_name = 'Employees'

# Create or get the database
database = client.create_database_if_not_exists(id=database_name)

# Create or get the container
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path="/id"), 
    offer_throughput=400  # Adjust throughput as needed
)

# Function to generate mock employee data
def generate_mock_employee_data():
    id = str(uuid.uuid4())
    emp_id = f"EMP{random.randint(10000, 99999)}"
    first_names = ["John", "Jane", "Alice", "Bob", "Charlie"]
    last_names = ["Doe", "Smith", "Johnson", "Brown", "Williams"]
    departments = ["Engineering", "HR", "Marketing", "Sales", "Finance"]

    return {
        "id": id,
        "emp_id": emp_id,
        "first_name": random.choice(first_names),
        "last_name": random.choice(last_names),
        "department": random.choice(departments)
    }

# Insert mock data every 5 seconds
try:
    while True:
        employee_data = generate_mock_employee_data()
        container.create_item(body=employee_data)
        print(f"Inserted document: {employee_data}")
        time.sleep(5) 
except KeyboardInterrupt:
    print("Data insertion stopped by user.")
except exceptions.CosmosHttpResponseError as e:
    print(f"An error occurred: {e.message}")
finally:
    client.close()

