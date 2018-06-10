FROM python:3.6
WORKDIR /usr/src/app
COPY req_retrain.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
