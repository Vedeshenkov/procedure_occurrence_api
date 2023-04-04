# Dockerfile

FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

COPY requirements.txt .

RUN pip install -r requirements.txt
RUN pip install --upgrade google-api-python-client

COPY ./app /app
