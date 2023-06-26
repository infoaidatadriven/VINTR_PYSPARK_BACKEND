FROM python:3.8-slim-buster
WORKDIR /app
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_ENV=production
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
CMD [ "flask","run"]