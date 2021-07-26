# DOSE Project - Modules MQTT
The repository contains the Beta version of the modules for the descentralized control of a battery using a MQTT protocol.

###  Building docker images
The docker images should be build from the parent directory _./modules_mqtt_ using:

* `docker build -t controller_slim -f ./docker_files/module_control/Dockerfile .`
* `docker build -t battery_slim -f ./docker_files/module_battery/Dockerfile .`
* `docker build -t forecast_slim -f ./docker_files/module_forecast/Dockerfile .`

The images can also be checked in docker hub [here.](https://hub.docker.com/repository/docker/salazaem/modules_mqtt "Docker hub")

### Architecture
An overview of the architecture of of the project:
 
![Architecture](https://github.com/MauricioSalazare/modules_mqtt/blob/master/docs/Architecture_v2.png)