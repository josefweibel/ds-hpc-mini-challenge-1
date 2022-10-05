import csv
import time
from message_service import MessageProducer
from movies_pb2 import Movie


producer = MessageProducer('movie_data_producer')

with open('./data/movies.csv', 'r') as f:
	reader = csv.reader(f)
	for i, line in enumerate(reader):
		if i == 0:
			continue

		movie = Movie()
		movie.id = int(line[0])
		movie.title = line[1]
		movie.genres.extend(line[2].split('|'))

		if i % 10 == 0:
			print(f'Publishing message {i}')

		producer.publish('movies', movie)

		time.sleep(0.1)

