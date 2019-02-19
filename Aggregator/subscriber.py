import os
from google.cloud import pubsub_v1

project = "axiom-227418"
topic_name = "features"
sub_name = "checker"

subscriber = pubsub_v1.SubscriberClient()
topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id=project,
    topic=topic_name
)

subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
    project_id=project,
    sub=sub_name
)
# subscriber.create_subscription(name=subscription_name, topic=topic_name)

def callback(message):
    print(message.data)
    message.ack()

future = subscriber.subscribe(subscription_name, callback)

try:
    future.result()
except KeyboardInterrupt:
    future.cancel()