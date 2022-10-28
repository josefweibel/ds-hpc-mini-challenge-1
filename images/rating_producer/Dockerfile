FROM python:3
RUN pip install kafka-python protobuf pika
COPY src/setup.py /setup.py
COPY src/rating_producer.py /rating_producer.py
COPY src/message_service.py /message_service.py
COPY src/movies_pb2.py /movies_pb2.py
COPY data/ratings.csv /data/ratings.csv

CMD python /setup.py && python /rating_producer.py
