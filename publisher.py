import os
import time
from google.cloud import pubsub_v1

# When you publish a message, the client returns a future.


# Initialize a Publisher client.
client = pubsub_v1.PublisherClient()
# Create a fully qualified identifier of form `projects/{project_id}/topics/{topic_id}
project_id='potent-bloom-299523'
topic_id= 'TopicDemo'
topic_path = client.topic_path(project_id, topic_id)
print(topic_path)
# Data sent to Cloud Pub/Sub must be a bytestring.
#data = b"Hello, World!"
input_file='Sales_record.csv'


with open(input_file, 'rb') as ifp:
    # skip header
    header = ifp.readline().splitlines()

    # loop over each record
    for line in ifp:
        event_data = line.rstrip()  # entire line of input CSV is the message
        api_future = client.publish(topic_path, event_data)
        message_id = api_future.result()
        print(f'Publishing\n {event_data} to {topic_path}: {message_id}')
        client.publish(topic_path, event_data)
        time.sleep(2)
# When you publish a message, the client returns a future.
# api_future = client.publish(topic_path, data)
# message_id = api_future.result()
#
# print(f"Published {data} to {topic_path}: {message_id}")
