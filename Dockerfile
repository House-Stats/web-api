FROM python:3.10.8

WORKDIR /app

COPY ./requirements.txt ./

RUN python3 -m pip install --upgrade pip setuptools wheel
RUN python3 -m pip install -r requirements.txt

COPY ./web .

CMD ["gunicorn", "-w 4", "-b 0.0.0.0:8000", "run:app"]
