import pandas as pd
import sys

from message_service import MessageProcessor, MessageProducer
from movies_pb2 import Rating, MeanRating
from storable_dataframe import StorableDataFrame

class MeanRatingProducer(MessageProcessor):
	def __init__(self, topics):
		super().__init__('mean_rating_producer', lambda t: Rating(), topics)
		self.producer = MessageProducer('mean_rating_producer')
		self.mean_ratings = StorableDataFrame('./data/mean-ratings.csv', 'movie_id', self.commit_async)

	def on_message(self, message, topic, kafka_message):
		self.mean_ratings.update(lambda df: self.update_mean_rating(df, message), kafka_message)

	def update_mean_rating(self, mean_ratings, message):
		if message.movieId in mean_ratings.index:
			movie = mean_ratings.loc[message.movieId]
		else:
			movie = pd.Series({'sum_ratings': 0, 'n': 0})

		n = movie.n + 1
		mean_rating = (message.value + movie.sum_ratings) / n

		mean_ratings.loc[message.movieId, 'n'] = n
		mean_ratings.loc[message.movieId, 'sum_ratings'] = movie.sum_ratings + message.value

		msg = MeanRating()
		msg.value = mean_rating
		msg.movieId = message.movieId
		self.producer.publish('mean-ratings', msg, key = str(message.movieId))

		print(f'Calculated rating {mean_rating} for movie {message.movieId}')

producer = MeanRatingProducer(['ratings'])
if __name__ == '__main__':
	if len(sys.argv) > 1:
		producer.run(n = int(sys.argv[1]))
	else:
		producer.run()
