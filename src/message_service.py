# change messaging system to use here:
system = 'rabbitmq' # or 'kafka'

if system == 'kafka':
	from kafka import KafkaConsumer, KafkaProducer
	import uuid

	servers = ['broker1:9093', 'broker2:9095', 'broker3:9097']

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

elif system == 'rabbitmq':
	import pika

	server = pika.ConnectionParameters('rabbitmq1', 5672)

	class MessageProducer:
		def __init__(self, name):
			self.con = pika.BlockingConnection(server)
			self.channel = self.con.channel()

		def publish(self, topic, message, key=None):
			self.channel.queue_declare(queue=topic)
			self.channel.basic_publish(
				exchange='',
				routing_key=topic,
				body=message.SerializeToString()
			)

			return None

	class MessageProcessor:
		def __init__(self, name, create_fnc, topics):
			self.create_fnc = create_fnc
			self.topics = topics
			self.con = pika.SelectConnection(server, on_open_callback=lambda c: self.__on_open(c))

		def run(self):
			self.con.ioloop.start()

		def __on_open(self, con):
			con.channel(on_open_callback=lambda c: self.__on_channel_open(c))

		def __on_channel_open(self, channel):
			for topic in self.topics:
				channel.queue_declare(queue=topic)
				self.__subscribe(channel, topic)

		# must be in a new function in order the topic won't get overriden in the next iteration of the for loop in __on_channel_open
		def __subscribe(self, channel, topic):
			channel.basic_consume(
				queue=topic,
				on_message_callback=lambda c, m, p, b: self.__on_message(topic, b),
				auto_ack=True
			)

		def __on_message(self, topic, body):
			try:
				parsed_message = self.create_fnc(topic)
				parsed_message.ParseFromString(body)
				self.on_message(parsed_message, topic)
			except Exception as e:
				print('cannot process message for topic ' + topic)

		def on_message(self, message, topic):
			raise NotImplementedError('on_message must be implemented')
else:
	raise ValueError('Unknown system ' + system)
