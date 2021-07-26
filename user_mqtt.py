import paho.mqtt.client as mqtt
from pyomo.environ import *
import pandas as pd
import json
import time
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from commons.parameters import BatteryParameters, ControlParameters, Topics, bcolors

class UserModule:

    def __init__(self):
        self.battery_params = {BatteryParameters.NOMINAL_ENERGY: 30,
                               BatteryParameters.MAX_POWER_DISCHARGE: 6,
                               BatteryParameters.MAX_POWER_CHARGE: 6,
                               BatteryParameters.DELTA_T: 0.25,
                               BatteryParameters.SOC_MIN: 0.2,
                               BatteryParameters.SOC_MAX: 1.0,
                               BatteryParameters.EFFICIENCY: 1.0,
                               BatteryParameters.SOC_INI_ACTUAL: 0.8}
        # Default controller settings: (This data should come from the user module)
        self.controller_params = {ControlParameters.POWER_THRESHOLD: 6,
                                  ControlParameters.OPTIMIZER_WINDOW: 96}

        self.solutions = pd.DataFrame([])
        self.real_power = pd.DataFrame([])
        self.predicted_power = pd.DataFrame([])
        self.continue_simulation = True


class UserMQTT(UserModule, mqtt.Client):

    def __init__(self,
                 control_id,
                 controlled_sensor_id,
                 controlled_phase_id,
                 user_id_mqtt,
                 mqtt_server_ip,
                 mqtt_server_port):
        UserModule.__init__(self)
        mqtt.Client.__init__(self, client_id=user_id_mqtt)

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
            self.solutions = pd.concat([self.solutions, received_message_frame.iloc[[0], :]])

        elif msg.topic == self.topics.forecast_topic:
            received_message = msg.payload.decode('utf-8')
            received_message_frame = pd.read_json(received_message,
                                                  convert_dates=[ControlParameters.DATE_STAMP_OPTIMAL])
            received_message_frame = received_message_frame.set_index(ControlParameters.DATE_STAMP_OPTIMAL, drop=True)
            self.predicted_power = pd.concat([self.predicted_power, received_message_frame.iloc[[0], :]])

        elif msg.topic == self.topics.sensor_topic:
            received_message = msg.payload.decode('utf-8')
            received_message_frame = pd.read_json(received_message,
                                                  convert_dates=[ControlParameters.DATE_STAMP_OPTIMAL])
            received_message_frame = received_message_frame.set_index(ControlParameters.DATE_STAMP_OPTIMAL, drop=True)
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
    mosquitto_server_ip = "localhost"
    mosquitto_server_port = 1883
    user_module_mqtt = UserMQTT(control_id=1,
                                controlled_sensor_id='gebouw',
                                controlled_phase_id='l1',
                                user_id_mqtt="USER",
                                mqtt_server_ip=mosquitto_server_ip,
                                mqtt_server_port=mosquitto_server_port)

    # while True:
    continue_simulation = True
    while continue_simulation:
        user_module_mqtt.process_mqtt_messages()
        continue_simulation = user_module_mqtt.check_stop_simulation()


#%% Plotting the simulations
    solutions_frame = user_module_mqtt.solutions

    real_power = user_module_mqtt.real_power.values.ravel()
    predicted_power = user_module_mqtt.predicted_power.values.ravel()
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







