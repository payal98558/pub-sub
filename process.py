import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
#from apache_beam import window


# Replace 'my-service-account-path' with your service account path
# service_account_path = 'C:\pipeline-service-account\My First Project-f8f6ef9fd8de.json'
# print("Service account file : ", service_account_path)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

input_subscription = 'projects/potent-bloom-299523/subscriptions/subscribeDemo'

output_topic = 'projects/potent-bloom-299523/topics/topic2'


# options = PipelineOptions()
# options.view_as(StandardOptions).streaming = True

pipeline_options = PipelineOptions(
     streaming=True, save_main_session=True
)

p = beam.Pipeline(options=pipeline_options)


output_file = 'outputs/part'

pubsub_data = (
                p
                | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription = input_subscription)
                | 'Write to pus sub' >> beam.io.WriteToPubSub(output_topic)
              )

result = p.run()
result.wait_until_finish()
