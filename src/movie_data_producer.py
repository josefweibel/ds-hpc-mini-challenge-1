import csv
import time
import math
import sys
from message_service import MessageProducer
from movies_pb2 import Movie


producer = MessageProducer('movie_data_producer')

def read_file():
	with open('./data/movies.csv', 'r') as f:
		reader = csv.reader(f)
		for line in reader:
			yield line

def publish(lines, n = math.inf, wait_time = 0.1):
	for i, line in enumerate(lines):
		if i == 0:
			continue

		if i > n:
			break

		movie = Movie()
		movie.id = int(line[0])
		movie.title = line[1]
		movie.genres.extend(line[2].split('|'))

		if i % 10 == 0:
			print(f'Publishing message {i}')

		producer.publish('movies', movie)

		time.sleep(wait_time)

if __name__ == '__main__':
	if len(sys.argv) > 1:
		publish(read_file(), n = int(sys.argv[1]))
	else:
		publish(read_file())
