FROM python:3
RUN pip install kafka-python protobuf pandas pika
COPY src/setup.py /setup.py
COPY src/mean_rating_producer.py /mean_rating_producer.py
COPY src/message_service.py /message_service.py
COPY src/movies_pb2.py /movies_pb2.py
COPY src/storable_dataframe.py /storable_dataframe.py
RUN mkdir /data
RUN echo "movie_id;sum_ratings;n" > /data/mean-ratings.csv

CMD python /setup.py && python /mean_rating_producer.py
