# Make sure to install below mentioned python packages
# pip install faker
# pip install azure-eventhub
from azure.eventhub import EventHubProducerClient, EventData
from faker import Faker
import json
import time
import random

# Replace these with your own values
CONNECTION_STR = "********************************************************************"
EVENT_HUB_NAME = "testeventhub"

# Initialize the Faker library to generate realistic mock data
fake = Faker()

# List of sample Starbucks products
products = [
    "Caffe Americano", "Caffe Latte", "Cappuccino", "Espresso", "Flat White",
    "Caramel Macchiato", "Mocha", "Iced Coffee", "Iced Latte", "Cold Brew",
    "Pumpkin Spice Latte", "Matcha Green Tea Latte", "Frappuccino"
]

# Function to create mock Starbucks order data
def create_mock_order_data():
    return {
        "order_id": fake.uuid4(),
        "store_id": fake.uuid4(),
        "store_location": {
            "city": fake.city(),
            "country": fake.country()
        },
        "customer_id": fake.uuid4(),
        "order_details": [
            {
                "product_name": random.choice(products),
                "quantity": random.randint(1, 5),
                "price": round(random.uniform(3, 10), 2)  # Price in USD
            }
        ],
        "order_total": round(random.uniform(5, 50), 2),  # Total order amount in USD
        "payment_method": random.choice(["Credit Card", "Debit Card", "Mobile Payment", "Cash"]),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

# Main function to send JSON data to Event Hub one by one in real time
def send_orders_data_to_event_hub():
    # Create a producer client to send messages to the event hub
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME
    )

    try:
        while True:
            # Generate mock data for Starbucks orders
            mock_data = create_mock_order_data()

            # Convert the mock data to JSON string
            json_data = json.dumps(mock_data)
            
            # Create an EventData object
            event_data = EventData(json_data)

            # Send the event to Event Hub
            producer.send_batch([event_data], partition_key=mock_data["order_id"])
            print(f"Successfully sent data: {json_data} with partition key: {mock_data['order_id']}")

            # Wait for 5 seconds before sending the next event
            time.sleep(5)

    except Exception as e:
        print(f"Error sending data: {e}")
    finally:
        # Close the producer
        producer.close()

if __name__ == "__main__":
    send_orders_data_to_event_hub()
