import pandas as pd
import sys

from message_service import MessageProcessor
from movies_pb2 import Movie, MeanRating
from storable_dataframe import StorableDataFrame

class MovieDataProcessor(MessageProcessor):
	def __init__(self, topics):
		super().__init__(
			'movie_data_sink',
			lambda t: Movie() if t == 'movies' else MeanRating(),
			topics
		)
		self.data = StorableDataFrame('./data/movie-data.csv', 'movie_id', self.commit_async)

	def on_message(self, message, topic, kafka_message):
		if topic == 'mean-ratings':
			self.save(message.movieId, {
				'mean_rating': message.value
			}, kafka_message)
		elif topic == 'movies':
			self.save(message.id, {
				'title': message.title,
				'genres': ','.join(message.genres)
			}, kafka_message)
		else:
			raise ValueError('unknown topic ' + topic)

	def save(self, id, data, kafka_message):
		self.data.update(lambda df: self.__save(df, id, data), kafka_message)

	def __save(self, data, id, new_data):
		keys = ', '.join(new_data.keys())
		print(f'Updating properties {keys} for movie {id}')

		for key, value in new_data.items():
			data.loc[id, key] = value

processor = MovieDataProcessor(['movies', 'mean-ratings'])
if __name__ == '__main__':
	if len(sys.argv) > 1:
		processor.run(n = int(sys.argv[1]))
	else:
		processor.run()
