FROM jupyter/scipy-notebook:${JUPYTER_VERSION:-latest}
RUN pip install kafka-python protobuf pandas pika snakeviz memory-profiler
COPY src /home/jovyan/
RUN mkdir /home/jovyan/data
COPY data/ratings.csv /home/jovyan/data/ratings.csv
COPY data/movies.csv /home/jovyan/data/movies.csv
RUN echo "movie_id;sum_ratings;n" > /home/jovyan/data/mean-ratings.csv
RUN echo "movie_id;title;genres;mean_rating" > /home/jovyan/data/movie-data.csv
