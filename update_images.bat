@echo off
:: Section 1: Build the images
ECHO ============================
ECHO BUILDING DOCKER IMAGES
ECHO ============================
docker build -t controller_x -f ./docker_files/module_control/Dockerfile .
docker build -t battery_x -f ./docker_files/module_battery/Dockerfile .
docker build -t dbmanager_x -f ./docker_files/module_dbmanager/Dockerfile .
docker build -t forecast_x -f ./docker_files/module_forecast/Dockerfile .

:: Section 2: Changing tag names
ECHO ============================
ECHO CHANGE TAG NAMES
ECHO ============================
docker tag dbmanager_x salazaem/modules_mqtt_v2:dbmanager_x
docker tag forecast_x salazaem/modules_mqtt_v2:forecast_x
docker tag battery_x salazaem/modules_mqtt_v2:battery_x
docker tag controller_x salazaem/modules_mqtt_v2:controller_x

:: Section 3: Pushing images to hub.docker.com
ECHO ============================
ECHO PUSHING IMAGES TO HUB
ECHO ============================
docker push salazaem/modules_mqtt_v2:dbmanager_x
docker push salazaem/modules_mqtt_v2:forecast_x
docker push salazaem/modules_mqtt_v2:battery_x
docker push salazaem/modules_mqtt_v2:controller_x

ECHO Done!!
PAUSE