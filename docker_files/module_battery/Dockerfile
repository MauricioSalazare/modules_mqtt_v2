FROM python:3.8.6-slim

MAINTAINER MauricioSalazar e.m.salazar.duque@tue.nl

WORKDIR /usr/docker/app

COPY /docker_files/module_battery/requirements_battery.txt .
COPY /commons/influxDB_to_icarus.py ./commons/
COPY /commons/parameters.py ./commons/
COPY /commons/SMABattery.py ./commons/
COPY battery_mqtt.py .
COPY /docker_files/module_battery/install_pysunspec.sh .

RUN pip install --no-cache-dir -r requirements_battery.txt
RUN bash install_pysunspec.sh

EXPOSE 1883

ENTRYPOINT ["python", "battery_mqtt.py"]
CMD ["-H localhost", "-P 1883", "-c BATTERY_1", "-B 1", "--mode 0"]