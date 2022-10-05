FROM python:3
RUN pip install kafka-python protobuf pika
COPY src/setup.py /setup.py
COPY src/movie_data_producer.py /movie_data_producer.py
COPY src/message_service.py /message_service.py
COPY src/movies_pb2.py /movies_pb2.py
COPY data/movies.csv /data/movies.csv

CMD python /setup.py && python /movie_data_producer.py
