FROM python:3.11-slim

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY .env .env

COPY script.py script.py

ENTRYPOINT [ "python", "script.py" ]