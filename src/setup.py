from typing import final
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

admin_client = KafkaAdminClient(
    bootstrap_servers='broker1:9093',
	client_id='setup',
	api_version=(0, 10),
	reconnect_backoff_max_ms=10000
)

try:
	admin_client.create_topics(
		new_topics=[
			NewTopic(name='movies', num_partitions=1, replication_factor=3),
			NewTopic(name='ratings', num_partitions=1, replication_factor=3),
			NewTopic(name='mean-ratings', num_partitions=1, replication_factor=3)
		],
		validate_only=False
	)
except TopicAlreadyExistsError as ex:
	pass
except Exception as ex:
	raise ex
finally:
	admin_client.close()
