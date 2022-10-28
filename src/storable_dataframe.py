import threading
import time
import pandas as pd
from kafka import OffsetAndMetadata, TopicPartition

class StorableDataFrame:
	def __init__(self, filename, index_col, commit_fnc, save_interval = 5):
		self.df = pd.read_csv(filename, sep = ';', index_col = index_col)
		self.filename = filename
		self.save_interval = save_interval

		self.commit_fnc = commit_fnc
		self.offsets = {}

		self.lock = threading.Lock()

		self.save_thread = threading.Thread(target=self.run)
		self.save_thread.start()

	def update(self, fnc, kafka_message):
		"""
		After calling this function for a message, the message is considered commited.
		"""
		with self.lock:
			fnc(self.df)

			# meta can be None: https://stackoverflow.com/a/36580451
			self.offsets[TopicPartition(kafka_message.topic, kafka_message.partition)] = OffsetAndMetadata(kafka_message.offset + 1, None)

	def run(self):
		main_thread = threading.main_thread()
		while main_thread.is_alive():
			time.sleep(self.save_interval)

			with self.lock:
				if not self.offsets:
					continue

				df = self.df.copy()
				offsets = self.offsets
				self.offsets = {}

			df.to_csv(self.filename, sep = ';')
			self.commit_fnc(offsets)
			print(f'Stored latest data in {self.filename}')
