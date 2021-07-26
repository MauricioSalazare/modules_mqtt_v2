import pandas as pd
from commons.influxDB_to_icarus import influxToIcarus
import datetime
import json
import paho.mqtt.client as mqtt
import time
from commons.parameters import Topics
import argparse


class ForecastIcarus:
    def __init__(self, id_sensor, phase=None, use_forecast=True):
        """Simulate the prediction database"""

        if phase is not None:
            assert phase in ['l1', 'l2', 'l3'], 'Incorrect phase description'

        self.id_sensor = id_sensor
        self.phase = phase
        self.use_forecast = use_forecast

        # Days to simulate the battery operation
        DAYS_PAST = 6  # Maximum 7 days in the past, otherwise icarus throws an error
        DAYS_FUTURE = 3

        date_init = datetime.datetime.now() - datetime.timedelta(DAYS_PAST)
        date_now = datetime.datetime.now()

        # Round the current time to the nearest 15 minutes quarter of the day.
        date_init = datetime.datetime(date_init.year, date_init.month, date_init.day, date_init.hour,
                                      15 * (date_init.minute // 15))
        date_now = datetime.datetime(date_now.year, date_now.month, date_now.day, date_now.hour,
                                     15 * (date_now.minute // 15))

        
        date_init_ = pd.to_datetime(date_init).tz_localize('Europe/Amsterdam').\
                                               tz_convert('UTC').isoformat().replace('+00:00', 'Z')
        date_now_ = pd.to_datetime(date_now).tz_localize('Europe/Amsterdam'). \
                                             tz_convert('UTC').isoformat().replace('+00:00', 'Z')
                
        # Connection to the influxDB and Icarus
        auth_user = 'welcometoicarus42'
        auth_password = 'AfFXygThfGBOBIPiOZBkJjxx'
        API_KEY = '80bada96-dd38-4916-91a2-5e832bae8124-tue_elaad'
        sand_box = False

        connection = influxToIcarus(influx_host='trafo.elaad.io',
                                    influx_port=8086,
                                    influx_user='bas',
                                    influx_password='n0rPQwz8iyEez^jn*$$@BR$paYP720bG',
                                    icarus_user=auth_user,
                                    icarus_password=auth_password,
                                    icarus_api_key=API_KEY,
                                    sand_box=sand_box)

        # TODO: Figure out to publish the data of a list of sensors and phases
        real_power = connection.get_influx_data(id_sensor=id_sensor,
                                                    date_init=date_init_,
                                                    date_end=date_now_,
                                                    phase=phase,
                                                    in_kw=True)
        forecast_power = connection.get_icarus_forecast(from_date=date_init.strftime('%Y-%m-%d'),
                                                  days=DAYS_PAST + DAYS_FUTURE,
                                                  phase=phase,
                                                  in_kw=True)
        self.join_time_series = pd.concat([real_power, forecast_power], axis=1).dropna(axis=0, how='any')
        self.date_init = date_init
        self.date_end = date_now

    def get_prediction(self, time, N):
        # TODO: Figure out how to standardize for multiple phases and/or sensors
        if self.phase is None:  # Work around to get one phase only
            phase = 'l1'
        else:
            phase = self.phase

        # Instead of rolling on each call, just make shorter the data frame
        power_real = self.join_time_series[phase][time:(time + N)]
        power_prediction = self.join_time_series['forecast_' + phase][time:(time + N)]

        return power_real, power_prediction


class ForecastMQTT(ForecastIcarus, mqtt.Client):
    """
    This class handles the subscription and publications of the Forecasting module.
    """

    def __init__(self,
                 id_sensor,
                 phase,
                 client_id_mqtt,
                 mqtt_server_ip,
                 mqtt_server_port,
                 use_forecast=True):
        ForecastIcarus.__init__(self, id_sensor=id_sensor, phase=phase, use_forecast=use_forecast)
        mqtt.Client.__init__(self, client_id=client_id_mqtt)

        self.time_counter = 0
        self.temporary_window = 100
        self.topics = Topics

        # Publish
        self.topics.forecast_topic += f"{id_sensor}_{phase}"
        self.topics.sensor_topic += f"{id_sensor}_{phase}"

        self.qos = 1
        self.connect(host=mqtt_server_ip,
                     port=mqtt_server_port)

    def reset_time_counter(self):
        self.time_counter = 0

    def on_connect(self, mqtt, obj, flags, rc):
        print("Connected with result code " + str(rc))

    def on_message(self, client, userdata, msg):
        print(f"Message received on topic: {msg.topic}")

    def publish_response(self):
        """Every time it is called, the prediction is rolled one time step"""
        (power_real,
         power_prediction) = self.get_prediction(int(self.time_counter), int(self.temporary_window))
        self.time_counter = self.time_counter + 1

        message_real_power = power_real.reset_index().to_json(date_format='iso')
        message_predicted_power = power_prediction.reset_index().to_json(date_format='iso')

        if self.use_forecast:
            message_to_controller = message_predicted_power
        else:
            message_to_controller = message_real_power

        print(f"Publishing topic: {self.topics.forecast_topic}")
        print(f"Publishing payload: {message_to_controller}")
        self.publish(topic=self.topics.forecast_topic, payload=message_to_controller)

        print(f"Publishing topic: {self.topics.sensor_topic}")
        print(f"Publishing payload: {message_real_power}")
        self.publish(topic=self.topics.sensor_topic, payload=message_real_power)


    def stop_simulation(self):
        self.publish(topic=self.topics.forecast_stop_simulation,
                     payload=json.dumps({'stop': True}))

    def process_mqtt_messages(self):
        self.loop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-H', '--host', required=False, type=str, default='localhost')
    parser.add_argument('-P', '--port', required=False, type=int, default=1883,
                        help='8883 for TLS or 1883 for non-TLS')
    parser.add_argument('-c', '--clientid', required=False, default="FORECAST",
                        help="Client id for the mosquitto server")
    parser.add_argument('-S', '--sensorid', required=False, default='gebouw',
                        help="Name of the sensor measurement")
    parser.add_argument('-L', '--phaseid', required=False, default='l1',
                        help="Phase of the sensor measurement e.g., 'l1', 'l2' or 'l3'")
    parser.add_argument('-F', '--forecast', required=False, default=True, action='store_false',
                        help="Use this flag to disable the forecast and use real consumption")

    args, unknown = parser.parse_known_args()

    print(f"ip address: {args.host}")
    print(f"Port: {args.port}")
    print(f"Client id: {args.clientid}")
    print(f"Sensor id: {args.sensorid}")
    print(f"Phase id: {args.phaseid}")
    print(f"Use forecast: {args.forecast}")

    forecast_module_mqtt = ForecastMQTT(id_sensor=args.sensorid,
                                        phase=args.phaseid,
                                        client_id_mqtt=args.clientid,
                                        mqtt_server_ip=args.host,
                                        mqtt_server_port=args.port,
                                        use_forecast=args.forecast)

    ii = 0
    total_iterations = len(forecast_module_mqtt.join_time_series.index) - 100
    while ii < total_iterations:
        print(f"Iteration: {ii} of {total_iterations - 1}")
        forecast_module_mqtt.process_mqtt_messages()
        forecast_module_mqtt.publish_response()
        time.sleep(0.25)  # Half second simulation speed
        ii = ii + 1

    forecast_module_mqtt.stop_simulation()
    forecast_module_mqtt.reset_time_counter()