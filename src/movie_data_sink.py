import pandas as pd

from message_service import MessageProcessor
from movies_pb2 import Movie, MeanRating

class MovieDataSink:
	def __init__(self):
		self.data = self.__load_data()

	def __load_data(self):
		return pd.read_csv('./data/movie-data.csv', sep = ';', index_col = 'movie_id')

	def save(self, id, data):
		for key, value in data.items():
			self.data.loc[id, key] = value

		keys = ', '.join(data.keys())
		print(f'Storing properties {keys} for movie {id}')

		self.data.to_csv('./data/movie-data.csv', sep = ';')

class MovieDataProcessor(MessageProcessor):
	def __init__(self, topics, sink):
		super().__init__(
			'movie_data_producer',
			lambda t: Movie() if t == 'movies' else MeanRating(),
			topics
		)
		self.sink = sink

	def on_message(self, message, topic):
		if topic == 'mean-ratings':
			self.sink.save(message.movieId, {
				'mean_rating': message.value
			})
		elif topic == 'movies':
			self.sink.save(message.id, {
				'title': message.title,
				'genres': ','.join(message.genres)
			})
		else:
			raise ValueError('unknown topic ' + topic)

sink = MovieDataSink()
MovieDataProcessor(['movies', 'mean-ratings'], sink).run()
