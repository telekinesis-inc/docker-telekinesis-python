FROM python:3.9-slim-buster

WORKDIR /usr/src/app

RUN pip install telekinesis

COPY . .

ENTRYPOINT ["python", "./script.py"]