FROM python:3.8.6-slim

MAINTAINER MauricioSalazar e.m.salazar.duque@tue.nl

WORKDIR /usr/docker/app

COPY /docker_files/module_forecast/requirements_forecast.txt .
COPY /commons/influxDB_to_icarus.py ./commons/
COPY /commons/parameters.py ./commons/
COPY forecast_mqtt.py .

RUN pip install --no-cache-dir -r requirements_forecast.txt

EXPOSE 1883

ENTRYPOINT ["python", "forecast_mqtt.py"]
CMD ["-H localhost", "-P 1883", "-c FORECAST", "-S gebouw", "-L l1"]