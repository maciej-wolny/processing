import os
from google.cloud import pubsub_v1

if __name__ == '__main__':
    subscriber = pubsub_v1.SubscriberClient()
    topic_name = 'projects/ox-legacyaudience-prod/topics/legacyaudience-notifications'.format(
        project_id=os.getenv('ox-legacyaudience-prod'),
        topic='legacyaudience-notifications',  # Set this to something appropriate.
    )
    subscription_name = 'projects/ox-legacyaudience-prod/subscriptions/legacyaudience-notifications'.format(
        project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
        sub='legacyaudience-notifications',  # Set this to something appropriate.
    )
    messages = 0
    while messages < 10000:
        response = subscriber.pull('projects/ox-legacyaudience-prod/subscriptions/legacyaudience-notifications',
                                   max_messages=500)
        messages += 500
        for msg in response.received_messages:
            print("Received message:", msg.message.data)

    # ack_ids = [msg.ack_id for msg in response.received_messages]
    # subscriber.acknowledge(subscription_path, ack_ids)
