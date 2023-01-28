FROM python:3.10.8

WORKDIR /app

COPY ./web .
COPY ./requirements.txt ./


RUN python3 -m pip install --upgrade pip setuptools wheel
RUN python3 -m pip install psycopg2 pymongo confluent_kafka flask gunicorn flask-cors

CMD ["gunicorn", "-w 4", "-b 0.0.0.0:8000", "run:app"]
