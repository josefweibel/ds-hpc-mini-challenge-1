from kafka import KafkaConsumer, KafkaProducer
import uuid

servers = 'broker1:9093'

class MessageProducer:
	def __init__(self, name):
		self.kafka_producer = KafkaProducer(
			bootstrap_servers=servers,
			api_version=(0, 10),
			client_id=name
		)

	def publish(self, topic, message, key=str(uuid.uuid4())):
		key_bytes = bytes(key, encoding='utf-8')
		value_bytes = message.SerializeToString()

		self.kafka_producer.send(topic, key=key_bytes, value=value_bytes)
		self.kafka_producer.flush()

		return key

class MessageProcessor:
	def __init__(self, name, create_fnc, topics):
		self.create_fnc = create_fnc
		self.kafka_consumer = KafkaConsumer(
			auto_offset_reset='earliest',
			bootstrap_servers=servers,
			api_version=(0, 10),
			client_id=name
		)
		self.kafka_consumer.subscribe(topics)

	def run(self):
		for message in self.kafka_consumer:
			try:
				parsed_message = self.create_fnc(message.topic)
				parsed_message.ParseFromString(message.value)
				self.on_message(parsed_message, message.topic)
			except Exception as e:
				print('cannot process message for topic ' + message.topic)

	def on_message(self, message, topic):
		raise NotImplementedError('on_message must be implemented')
