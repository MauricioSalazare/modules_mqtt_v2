import pandas as pd
from commons.influxDB_to_icarus import influxToIcarus
import datetime
import json
import paho.mqtt.client as mqtt
import time
from commons.parameters import Topics
import argparse


class ForecastIcarus:
    DAYS_PAST = 6  # Maximum 7 days in the past, otherwise icarus throws an error
    DAYS_FUTURE = 3

    def __init__(self, id_sensor, phase=None, use_forecast=True, simulation_mode=0):
        """
        Simulation mode {0} uses data of the past e.g., from icarus or influx to test the system.
        Simulation mode {1} used data from the icarus e.g., takes current PC time and get the forecast of the next days.
        """


        # Connection to the influxDB and Icarus
        auth_user = 'welcometoicarus42'
        auth_password = 'AfFXygThfGBOBIPiOZBkJjxx'
        API_KEY = '80bada96-dd38-4916-91a2-5e832bae8124-tue_elaad'
        sand_box = False

        self.connection = influxToIcarus(influx_host='trafo.elaad.io',
                                         influx_port=8086,
                                         influx_user='bas',
                                         influx_password='n0rPQwz8iyEez^jn*$$@BR$paYP720bG',
                                         icarus_user=auth_user,
                                         icarus_password=auth_password,
                                         icarus_api_key=API_KEY,
                                         sand_box=sand_box)

        if phase is not None:
            assert phase in ['l1', 'l2', 'l3'], 'Incorrect phase description'

        self.id_sensor = id_sensor
        self.phase = phase
        self.use_forecast = use_forecast

        if not simulation_mode:
            self.load_data_for_simulation()

    def round_time(self, date_info):
        """Return the date-time stamp to the closest quarter of the day, and formats it for use into the icarus"""
        rounded_time = datetime.datetime(date_info.year, date_info.month, date_info.day, date_info.hour, 15 * (date_info.minute // 15))
        rounded_time_formatted = pd.to_datetime(rounded_time).tz_localize('Europe/Amsterdam').tz_convert('UTC').isoformat().replace('+00:00', 'Z')

        return (rounded_time, rounded_time_formatted)

    def load_data_for_simulation(self):
        """Ran only once. Load the past 6 days of data onto memory of the real consumption and the predicted from icarus"""
        date_now_cpu = datetime.datetime.now()
        date_init_cpu = date_now_cpu - datetime.timedelta(self.DAYS_PAST)

        (date_now, date_now_) = self.round_time(date_now_cpu)
        (date_init, date_init_) = self.round_time(date_init_cpu)

        real_power = self.connection.get_influx_data(id_sensor=self.id_sensor,
                                                    date_init=date_init_,
                                                    date_end=date_now_,
                                                    phase=self.phase,
                                                    in_kw=True)

        forecast_power = self.connection.get_icarus_forecast(from_date=date_init.strftime('%Y-%m-%d'),
                                                    days=self.DAYS_PAST + self.DAYS_FUTURE,
                                                    phase=self.phase,
                                                    in_kw=True)
        self.join_time_series = pd.concat([real_power, forecast_power], axis=1).dropna(axis=0, how='any')
        self.date_init = date_init
        self.date_end = date_now

    def get_prediction_for_simulation(self, time, N):
        """Return a slide window of the real consumption and the predicted one for the last 6 dyas"""
        if self.phase is None:  # Work around to get one phase only
            phase = 'l1'
        else:
            phase = self.phase

        power_real = self.join_time_series[phase][time:(time + N)]
        power_prediction = self.join_time_series['forecast_' + phase][time:(time + N)]

        return power_real, power_prediction

    def get_prediction(self):
        # Indexing to slice the prediction from icarus

        date_now_cpu = datetime.datetime.now()
        (date_now, date_now_) = self.round_time(date_now_cpu)

        ts_future = datetime.timedelta(minutes=0)

        the_data = self.connection.get_icarus_forecast(from_date=date_now.strftime('%Y-%m-%d'),
                                                      days=self.DAYS_FUTURE,
                                                      phase=self.phase,
                                                      in_kw=True)

        # Indexes to slide the future time_steps
        data_now_datetimme = pd.to_datetime(date_now, utc=True)
        data_now_datetimme_end = data_now_datetimme + datetime.timedelta(self.DAYS_FUTURE)

        forecast = the_data.loc[(data_now_datetimme+ts_future):data_now_datetimme_end].dropna(axis=0, how='any')  #TODO: This could create holes

        return forecast







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
                 simulation_mode,
                 use_forecast=True):
        ForecastIcarus.__init__(self, id_sensor=id_sensor, phase=phase, use_forecast=use_forecast, simulation_mode=simulation_mode)
        mqtt.Client.__init__(self, client_id=client_id_mqtt)

        self.simulation_mode = simulation_mode
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
        if not self.simulation_mode:
            (power_real,
             power_prediction) = self.get_prediction_for_simulation(int(self.time_counter), int(self.temporary_window))
            self.time_counter = self.time_counter + 1

        else:
            power_real = self.get_prediction()
            power_prediction = power_real

        message_predicted_power = power_prediction.reset_index().to_json(date_format='iso')
        message_real_power = power_real.reset_index().to_json(date_format='iso')

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
        # This is only for the user module, to know when the simulation ended.
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
    parser.add_argument('--mode', required=False, type=int, default=1, choices={0, 1},
                        help="0: Enables simulation mode (data from past), 1: Uses only forecast from now (real operation).")
    parser.add_argument('--delay', required=False, type=float, default=0.25,
                        help="Delay time between simulation/forecast update.")

    args, unknown = parser.parse_known_args()

    print(f"ip address: {args.host}")
    print(f"Port: {args.port}")
    print(f"Client id: {args.clientid}")
    print(f"Sensor id: {args.sensorid}")
    print(f"Phase id: {args.phaseid}")
    print(f"Use forecast: {args.forecast}")
    print(f"Enable sma: {args.mode}")
    print(f"Simulation every: {args.delay}")
    delay = args.delay

    forecast_module_mqtt = ForecastMQTT(id_sensor=args.sensorid,
                                        phase=args.phaseid,
                                        client_id_mqtt=args.clientid,
                                        mqtt_server_ip=args.host,
                                        mqtt_server_port=args.port,
                                        use_forecast=args.forecast,
                                        simulation_mode=args.mode)

    if not args.mode:
        print("Simulation mode!!")
    else:
        print("SMA enabled. Controlling in real life with real forecast!!")

    if not args.mode:  # Simulation mode
        ii = 0
        total_iterations = len(forecast_module_mqtt.join_time_series.index) - 100
        while ii < total_iterations:
            print(f"Iteration: {ii} of {total_iterations - 1}")
            forecast_module_mqtt.process_mqtt_messages()
            forecast_module_mqtt.publish_response()
            time.sleep(delay)  # Half second simulation speed
            ii = ii + 1

        forecast_module_mqtt.stop_simulation()
        forecast_module_mqtt.reset_time_counter()

    else:  # Real operation: This should be an infinite loop. Now truncated to test. Uses icarus Forecast.
        ii = 0
        total_iterations = 10
        while ii < total_iterations:
        # while True:
            print(f"Iteration: {ii} of {total_iterations - 1}")
            forecast_module_mqtt.process_mqtt_messages()
            forecast_module_mqtt.publish_response()
            time.sleep(delay)  # Half second simulation speed
            ii = ii + 1

        forecast_module_mqtt.stop_simulation()
        forecast_module_mqtt.reset_time_counter()



