FROM python:3.8.6-slim

MAINTAINER MauricioSalazar e.m.salazar.duque@tue.nl

WORKDIR /usr/docker/app

COPY /docker_files/module_dbmanager/requirements_dbmanager.txt .
COPY /commons/timescaledb_connection.py ./commons/
COPY /commons/parameters.py ./commons/
COPY dbmanager_mqtt.py .

RUN apt-get update \
    && apt-get -y install libpq-dev gcc
RUN pip install --no-cache-dir -r requirements_dbmanager.txt

EXPOSE 1883
EXPOSE 5432

ENTRYPOINT ["python", "dbmanager_mqtt.py"]
CMD ["-H localhost", "-P 1883", "--dbip localhost", "--dbusername postgres", "--dbpassword postgres"]