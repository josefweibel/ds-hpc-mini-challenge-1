import csv
from message_service import MessageProducer
from movies_pb2 import Rating


producer = MessageProducer('rating_producer')

with open('./data/ratings.csv', 'r') as f:
	reader = csv.reader(f)
	for i, line in enumerate(reader):
		if i == 0:
			continue

		rating = Rating()
		rating.userId = int(line[0])
		rating.movieId = int(line[1])
		rating.value = float(line[2])

		if i % 100 == 0:
			print(f'Publishing message {i}')

		producer.publish('ratings', rating)
