import csv
import math
import sys
from message_service import MessageProducer
from movies_pb2 import Rating


producer = MessageProducer('rating_producer')

def read_file():
	with open('./data/ratings.csv', 'r') as f:
		reader = csv.reader(f)
		for line in reader:
			yield line

def publish(lines, n = math.inf):
	for i, line in enumerate(lines):
		if i == 0:
			continue

		if i > n:
			break

		rating = Rating()
		rating.userId = int(line[0])
		rating.movieId = int(line[1])
		rating.value = float(line[2])

		if i % 100 == 0:
			print(f'Publishing message {i}')

		producer.publish('ratings', rating)

if __name__ == '__main__':
	if len(sys.argv) > 1:
		publish(read_file(), n = int(sys.argv[1]))
	else:
		publish(read_file())