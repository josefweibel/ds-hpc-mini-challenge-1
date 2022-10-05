import pandas as pd

from message_service import MessageProcessor, MessageProducer
from movies_pb2 import Rating, MeanRating

class MeanRatingProducer(MessageProcessor):
	def __init__(self, topics):
		super().__init__('mean_rating_producer', lambda t: Rating(), topics)
		self.producer = MessageProducer('mean_rating_producer')
		self.mean_ratings = self.__load_mean_ratings()

	def __load_mean_ratings(self):
		return pd.read_csv('./data/mean-ratings.csv', sep = ';', index_col = 'movie_id')

	def on_message(self, message, topic):
		if message.movieId in self.mean_ratings.index:
			movie = self.mean_ratings.loc[message.movieId]
		else:
			movie = pd.Series({'sum_ratings': 0, 'n': 0})

		n = movie.n + 1
		mean_rating = (message.value + movie.sum_ratings) / n

		self.mean_ratings.loc[message.movieId, 'n'] = n
		self.mean_ratings.loc[message.movieId, 'sum_ratings'] = movie.sum_ratings + message.value

		print(f'Storing rating {mean_rating} for movie {message.movieId}')

		self.mean_ratings.to_csv('./data/mean-ratings.csv', sep = ';')

		msg = MeanRating()
		msg.value = mean_rating
		msg.movieId = message.movieId
		self.producer.publish('mean-ratings', msg)

MeanRatingProducer(['ratings']).run()
