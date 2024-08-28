# Make sure to install below mentioned python packages
# pip install azure-eventhub

from azure.eventhub import EventHubConsumerClient
# Event Hub connection details
connection_str="********************************************************"
eventhub_name = "testeventhub"
consumer_group = "$Default"

# Function to handle events received from the Event Hub
def on_event(partition_context, event):
    # Fetch the partition key (if available) and the event data (value)
    partition_key = event.partition_key
    event_data = event.body_as_str(encoding='UTF-8')

    # Print the partition key and the event data
    print(f"Received event from partition: {partition_context.partition_id}")
    print(f"Partition Key: {partition_key}")
    print(f"Event Data: {event_data}")

    # Checkpointing - to keep track of read messages and avoid reading them again
    partition_context.update_checkpoint(event)

def on_error(partition_context, error):
    # Called when an error occurs
    print(f"Error on partition {partition_context.partition_id}: {error}")

def on_partition_initialize(partition_context):
    # Called when the partition is initialized
    print(f"Partition: {partition_context.partition_id} has been initialized.")

def on_partition_close(partition_context, reason):
    # Called when a partition is closed
    print(f"Partition: {partition_context.partition_id} closed, reason: {reason}")

if __name__ == "__main__":
    # Create a consumer client
    client = EventHubConsumerClient.from_connection_string(
        conn_str=connection_str,
        consumer_group=consumer_group,
        eventhub_name=eventhub_name
    )

    try:
        # Start receiving events
        client.receive(
            on_event=on_event,
            on_error=on_error,
            on_partition_initialize=on_partition_initialize,
            on_partition_close=on_partition_close,
            starting_position="-1"  # Start from the beginning of the stream
        )
    except KeyboardInterrupt:
        print("Receiving has been stopped.")
    finally:
        # Close the client connection
        client.close()
