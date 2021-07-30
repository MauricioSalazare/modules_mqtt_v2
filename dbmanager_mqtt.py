import paho.mqtt.client as mqtt
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from commons.parameters import BatteryParameters, ControlParameters, Topics, bcolors
from commons.timescaledb_connection import TimescaledbConnection
import argparse


class DBManager:
    def __init__(self):
        self.solutions = pd.DataFrame([])
        self.real_power = pd.DataFrame([])
        self.predicted_power = pd.DataFrame([])
        self.continue_simulation = True


    def melt_dataframe(self, message_frame):
        """Return a dataframe with the PostgreSQL table format."""

        ## Optional way to melt the dataframe.
        # df_new = df.reset_index()
        # df_new = pd.melt(df_new, id_vars="datetimeFC", value_vars=df_new.drop('datetimeFC', axis=1), var_name="channel", value_name="values_channel")

        df_output = message_frame.stack(0).reset_index().rename(columns={"level_1": "channel", 0: "value"})
        df_output['channel'] = pd.Categorical(df_output['channel'].tolist(), ordered=True,
                                              categories=df_output['channel'].unique().tolist())
        df_output = df_output.sort_values(['channel', 'datetimeFC']).reset_index(drop=True)
        df_output['datetimeFC'] = df_output['datetimeFC'].astype(str)
        df_output = df_output.round(2)

        return df_output


class DBManagerMQTT(DBManager, mqtt.Client, TimescaledbConnection):

    def __init__(self,
                 control_id,
                 controlled_sensor_id,
                 controlled_phase_id,
                 user_id_mqtt,
                 mqtt_server_ip,
                 mqtt_server_port,
                 username_db,
                 password_db,
                 port_db=5432,
                 ip_db='localhost',
                 clear_table_db=True):
        DBManager.__init__(self)
        mqtt.Client.__init__(self, client_id=user_id_mqtt)
        TimescaledbConnection.__init__(self, username=username_db, password=password_db, host=ip_db, port=port_db, clear_table=clear_table_db)

        self.last_message_controller = []
        self.last_message_forecast = []
        self.last_message_sensor = []

        self.topics = Topics
        # Subscribe
        self.topics.controller_results += f"control_{control_id}"
        self.topics.forecast_topic += f"{controlled_sensor_id}_" + f"{controlled_phase_id}"
        self.topics.sensor_topic += f"{controlled_sensor_id}_" + f"{controlled_phase_id}"

        # Connect to the MQTT Mosquitto
        self.qos = 1
        self.connect(host=mqtt_server_ip,
                     port=mqtt_server_port)

    def on_connect(self, mqtt, obj, flags, rc):
        print("Connected with result code " + str(rc))

        # Subscriber topics
        print(f"Subscribing to: {self.topics.controller_results}")
        self.subscribe(self.topics.controller_results, self.qos)

        print(f"Subscribing to: {self.topics.forecast_topic}")
        self.subscribe(self.topics.forecast_topic, self.qos)

        print(f"Subscribing to: {self.topics.sensor_topic}")
        self.subscribe(self.topics.sensor_topic, self.qos)

        print(f"Subscribing to: {self.topics.forecast_stop_simulation}")
        self.subscribe(self.topics.forecast_stop_simulation, self.qos)

    def on_message(self, client, userdata, msg):
        print("--" * 100)
        print(bcolors.OKGREEN + f"Message received on topic: {msg.topic}" + bcolors.ENDC)

        if msg.topic == self.topics.controller_results:  # Received the solution of the optimization
            received_message = msg.payload.decode('utf-8')
            received_message_frame = pd.read_json(received_message,
                                                  convert_dates=[ControlParameters.DATE_STAMP_OPTIMAL])
            received_message_frame = received_message_frame.set_index(ControlParameters.DATE_STAMP_OPTIMAL, drop=True)
            # self.last_message_controller.append(received_message_frame)

            # Save message on database:
            df_output = self.melt_dataframe(received_message_frame.iloc[[0], :])
            self.insert_data(df_output)

            # Save message locally:
            self.solutions = pd.concat([self.solutions, received_message_frame.iloc[[0], :]])


        elif msg.topic == self.topics.forecast_topic:  # This data is also in controller_results
            received_message = msg.payload.decode('utf-8')
            received_message_frame = pd.read_json(received_message,
                                                  convert_dates=[ControlParameters.DATE_STAMP_OPTIMAL])
            received_message_frame = received_message_frame.set_index(ControlParameters.DATE_STAMP_OPTIMAL, drop=True)
            # self.last_message_forecast.append(received_message_frame)
            self.predicted_power = pd.concat([self.predicted_power, received_message_frame.iloc[[0], :]])

        elif msg.topic == self.topics.sensor_topic:
            received_message = msg.payload.decode('utf-8')
            received_message_frame = pd.read_json(received_message,
                                                  convert_dates=[ControlParameters.DATE_STAMP_OPTIMAL])
            received_message_frame = received_message_frame.set_index(ControlParameters.DATE_STAMP_OPTIMAL, drop=True)
            self.last_message_sensor.append(received_message_frame)

            # Save message on database:
            df_output = self.melt_dataframe(received_message_frame.iloc[[0], :])
            self.insert_data(df_output)

            # Save message locally:
            self.real_power = pd.concat([self.real_power, received_message_frame.iloc[[0], :]])

        elif msg.topic == self.topics.forecast_stop_simulation:
            print("---" * 200)
            print("SIMULATION STOPPED")
            print("---" * 200)
            self.continue_simulation = False

    def process_mqtt_messages(self):
        self.loop()

    def check_stop_simulation(self):
        return self.continue_simulation


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-H', '--host', required=False, type=str, default='localhost')
    parser.add_argument('-P', '--port', required=False, type=int, default=1883,
                        help='8883 for TLS or 1883 for non-TLS')
    parser.add_argument('-c', '--clientid', required=False, default="DBMANAGER",
                        help="Client id for the mosquitto server")
    parser.add_argument('-S', '--sensorid', required=False, default='gebouw',
                        help="Name of the sensor measurement")
    parser.add_argument('-L', '--phaseid', required=False, default='l1',
                        help="Phase of the sensor measurement e.g., 'l1', 'l2' or 'l3'")
    parser.add_argument('--dbip', required=False, type=str, default='localhost',
                        help="Username for PostrgreSQL database")
    parser.add_argument('--dbport', required=False, type=int, default=5432,
                        help="Username for PostrgreSQL database")
    parser.add_argument('--dbusername', required=False, type=str, default='postgres',
                        help="Username for PostrgreSQL database")
    parser.add_argument('--dbpassword', required=False, type=str, default='postgres',
                        help="Password for PostrgreSQL database")
    args, unknown = parser.parse_known_args()

    user_module_mqtt = DBManagerMQTT(control_id=1,
                                     controlled_sensor_id=args.sensorid,
                                     controlled_phase_id=args.phaseid,
                                     user_id_mqtt=args.clientid,
                                     mqtt_server_ip=args.host,
                                     mqtt_server_port=args.port,
                                     username_db=args.dbusername,
                                     password_db=args.dbpassword,
                                     port_db=args.dbport,
                                     ip_db=args.dbip)

    # while True:
    continue_simulation = True
    while continue_simulation:
        user_module_mqtt.process_mqtt_messages()
        continue_simulation = user_module_mqtt.check_stop_simulation()


#%% Plotting the simulations
    solutions_frame = user_module_mqtt.solutions

    real_power = user_module_mqtt.real_power.values.ravel()  # Actual power measured by influxDB
    predicted_power = user_module_mqtt.predicted_power.values.ravel()  # Power from icarus

    predicted_power_control = solutions_frame[ControlParameters.FORECAST_VALUES].values
    power_threshold_phase = solutions_frame[ControlParameters.POWER_THRESHOLD].values

    battery_set_points = solutions_frame[ControlParameters.BATTERY_POWER_OPTIMAL].values
    net_power = (real_power + battery_set_points)

    delta_building_consumption = real_power - power_threshold_phase
    delta_net_demand = net_power - power_threshold_phase
    delta_building_consumption[delta_building_consumption < 0] = 0
    delta_net_demand[delta_net_demand < 0] = 0

    hours = mdates.HourLocator(interval=2)  # Every 2 - hour
    days = mdates.DayLocator()  # Every day

    x_ = solutions_frame.index
    fig = plt.figure(figsize=(16, 7))
    gs = fig.add_gridspec(4, 1, hspace=0.5)
    ax1 = fig.add_subplot(gs[0:2, :])
    ax2 = fig.add_subplot(gs[2, :])
    ax3 = fig.add_subplot(gs[3, :])

    ax1.step(x_, battery_set_points, label='Battery demand', color='b')
    ax1.step(x_, net_power, label='Net demand real', color='r')
    ax1.step(x_, predicted_power, label='Building predicted consumption', color='g')
    ax1.step(x_, real_power, label='Building real consumption', color='orange')
    ax1.xaxis.set_major_locator(days)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
    ax1.xaxis.grid(True, which='major')
    ax1.tick_params(which='major', length=12, labelsize=9)
    ax1.xaxis.set_minor_locator(hours)
    ax1.xaxis.set_minor_formatter(mdates.DateFormatter('%H'))
    ax1.tick_params(which='minor', length=4, labelsize=6)
    ax1.set_ylabel("[kW]")

    ax1.step(x_, power_threshold_phase, label='Power Threshold', linestyle='-', linewidth=0.5, color='g')
    ax1.axhline(0, linestyle='--', linewidth=0.3, color='b')
    ax1.legend(loc='upper center', bbox_to_anchor=(0.5, 1.2), ncol=5)

    ax2.step(x_, delta_building_consumption, label='Delta building consumption', color='orange')
    ax2.step(x_, delta_net_demand, label='Delta net demand', color='r')
    ax2.legend(loc='upper center', fontsize=7, ncol=2)
    ax2.set_ylabel('[kW]')
    ax2.xaxis.set_major_locator(days)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
    ax2.xaxis.grid(True, which='major')
    ax2.tick_params(which='major', length=12, labelsize=9)
    ax2.xaxis.set_minor_locator(hours)
    ax2.xaxis.set_minor_formatter(mdates.DateFormatter('%H'))
    ax2.tick_params(which='minor', length=4, labelsize=6)
    ax2.set_ylabel("[kW]")

    ax3.step(x_, solutions_frame[BatteryParameters.SOC_INI_ACTUAL].values, color='darkblue', label='State of charge')
    ax3.step(x_, solutions_frame[BatteryParameters.SOC_MIN].values, linestyle='--', color='darkblue', linewidth=0.3)
    ax3.step(x_, solutions_frame[BatteryParameters.SOC_MAX].values, linestyle='-', color='darkblue', linewidth=0.3)
    ax3.set_ylim([-0.1, 1.1])
    ax3.legend(loc='lower right', fontsize=7)
    ax3.xaxis.set_major_locator(days)
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%y'))
    ax3.xaxis.grid(True, which='major')
    ax3.tick_params(which='major', length=12, labelsize=9)
    ax3.xaxis.set_minor_locator(hours)
    ax3.xaxis.set_minor_formatter(mdates.DateFormatter('%H'))
    ax3.tick_params(which='minor', length=4, labelsize=6)
    ax3.set_ylabel('[p.u]')
    plt.show()

    plt.savefig('simulation_figure.png')
    solutions_frame.to_csv("simulation_data.csv")





