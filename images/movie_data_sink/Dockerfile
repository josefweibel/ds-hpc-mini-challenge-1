FROM python:3
RUN pip install kafka-python protobuf pandas
COPY src/setup.py /setup.py
COPY src/movie_data_sink.py /movie_data_sink.py
COPY src/message_service.py /message_service.py
COPY src/movies_pb2.py /movies_pb2.py
RUN mkdir /data
RUN echo "movie_id;title;genres;mean_rating" > /data/movie-data.csv

CMD python /setup.py && python /movie_data_sink.py
