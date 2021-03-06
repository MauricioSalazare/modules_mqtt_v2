import paho.mqtt.client as mqtt
import pandas as pd
import json
import time
from commons.parameters import BatteryParameters, ControlParameters, Topics, bcolors
from commons.SMABattery import SMABattery
import argparse
import schedule
import datetime

"""
Mapping of the variables names between BatteryModule and SMAModule:

sma = OBJECT(battery inverter)
BatteryParameters.NOMINAL_ENERGY = None  --> No way to retrieve it from the battery?????
BatteryParameters.MAX_POWER_DISCHARGE = -sma_battery.MAX_DISCHARGE_VALUE
BatteryParameters.MAX_POWER_CHARGE = sma.MAX_CHARGE_VALUE
BatteryParameters.SOC_INI_ACTUAL = actual_battery_status["storage"]["ChaState"] / 100) * self.battery_params[BatteryParameters.NOMINAL_ENERGY]
"""



class BatteryModule:
    def __init__(self, id, enable_sma=False, modbus_ip=None, modbus_port=None):
        self.id = id
        self.battery_params = {BatteryParameters.NOMINAL_ENERGY: 15,
                               BatteryParameters.MAX_POWER_DISCHARGE: 4,
                               BatteryParameters.MAX_POWER_CHARGE: 6,
                               BatteryParameters.DELTA_T: 0.25,
                               BatteryParameters.SOC_MIN: 0.1,
                               BatteryParameters.SOC_MAX: 0.9,
                               BatteryParameters.EFFICIENCY: 1.0,
                               BatteryParameters.SOC_INI_ACTUAL: 0.5,
                               BatteryParameters.POWER_OUTPUT: 0.0}

        self.enable_sma = enable_sma
        self.power_output = 0  # Must be in kW
        self.emergency_solutions = None

        if self.enable_sma:
            # Get all the battery parameters from the first time.
            assert (modbus_ip is not None) and (modbus_port is not None),  "Provide a valid ip/port for the inverter."

            self.sma_battery = SMABattery(modbus_ip=modbus_ip, modbus_port=modbus_port)
            self.sma_battery.changePower(0)  # Power at 0 Watts during the initialization.

            actual_battery_status = self.sma_battery.readSMAValues()

            # Update internal state with the values from the battery
            self.power_output = actual_battery_status["inverter"]["W"] / 1000.0  # Read power output. (Must be 0)
            self.battery_params[BatteryParameters.POWER_OUTPUT] = actual_battery_status["inverter"]["W"] / 1000.0  # Read power output. (Must be 0)
            # self.battery_params[BatteryParameters.SOC_INI_ACTUAL] = (actual_battery_status["storage"]["ChaState"] / 100) * self.battery_params[BatteryParameters.NOMINAL_ENERGY]
            self.battery_params[BatteryParameters.SOC_INI_ACTUAL] = (actual_battery_status["storage"]["ChaState"] / 100)
            self.battery_params[BatteryParameters.MAX_POWER_DISCHARGE] = -self.sma_battery.MAX_DISCHARGE_VALUE
            self.battery_params[BatteryParameters.MAX_POWER_CHARGE] = self.sma_battery.MAX_CHARGE_VALUE


    def get_battery_parameters(self):
        if self.enable_sma:
            # Update the battery parameters with data from the SMA inverter.
            # TODO: Create a thread inside SMABattery class, so it keeps an updated version of the SMA values in a dictionary
            actual_battery_status = self.sma_battery.readSMAValues()
            self.power_output = actual_battery_status["inverter"]["W"] / 1000.0
            self.battery_params[BatteryParameters.POWER_OUTPUT] = actual_battery_status["inverter"]["W"] / 1000.0
            # self.battery_params[BatteryParameters.SOC_INI_ACTUAL] = (actual_battery_status["storage"]["ChaState"] / 100) * self.battery_params[BatteryParameters.NOMINAL_ENERGY]
            self.battery_params[BatteryParameters.SOC_INI_ACTUAL] = (actual_battery_status["storage"]["ChaState"] / 100)
            self.battery_params[BatteryParameters.MAX_POWER_DISCHARGE] = -self.sma_battery.MAX_DISCHARGE_VALUE / 1000.0
            self.battery_params[BatteryParameters.MAX_POWER_CHARGE] = self.sma_battery.MAX_CHARGE_VALUE / 1000.0

            print(f"Current SoC from SMA: {self.battery_params[BatteryParameters.SOC_INI_ACTUAL]}")
            print(f"Current POWER OUTPUT from SMA: {self.power_output}  [kW]")
            print(f"Current POWER OUTPUT from SMA: {self.battery_params[BatteryParameters.POWER_OUTPUT]}  [kW]")

        return self.battery_params

    def update_battery_parameters(self, **kwargs):
        """Update the set point from the USER module. e.g., soc_min, soc_max, efficiency, etc."""

        if set(kwargs.keys()).issubset(BatteryParameters.VALID_KEYS):
            self.battery_params.update(kwargs)
            print(f"Parameters updated: {kwargs}")
        else:
            print("One of the dictionary keys is not valid")

    def set_emergency_solutions(self, emergency_solutions):
        """Get the last optimal set points in case the battery lost connection"""
        assert isinstance(emergency_solutions, pd.DataFrame)

        self.emergency_solutions = emergency_solutions

    def set_power_output(self, new_power_output):  # Value in kW
        max_power_discharge = self.battery_params[BatteryParameters.MAX_POWER_DISCHARGE]
        max_power_charge = self.battery_params[BatteryParameters.MAX_POWER_CHARGE]

        if ((new_power_output >= -max_power_discharge) & (new_power_output <= max_power_charge)):
            if self.enable_sma:
                self.sma_battery.changePower(new_power_output * 1000)  # Power in Watts to the inverter of the battery
                print(f"Power output updated on the inverter: {new_power_output} [kW]")
            else:
                print(f"Power output updated: {new_power_output} [kW]")

            self.power_output = new_power_output
            self.battery_params[BatteryParameters.POWER_OUTPUT] = new_power_output
        else:
            print('New power output outside battery limits. OUTPUT POWER WAS NOT UPDATED!!!')

    def simulate_battery_operation(self, delta_t_sim=1):
        """
        This will simulate the battery charge or discharge
        delta_t_sim is used in the stand alone simulation.
        """
        assert self.enable_sma is not True,  "Inverter mode is selected. Can not simulate"

        current_charge = self.battery_params[BatteryParameters.SOC_INI_ACTUAL]
        # delta_change_charge = ((self.battery_params[BatteryParameters.EFFICIENCY]
        #                         * self.power_output * (self.battery_params[BatteryParameters.DELTA_T] / delta_t_sim))
        #                         / self.battery_params[BatteryParameters.NOMINAL_ENERGY])
        delta_change_charge = ((self.battery_params[BatteryParameters.EFFICIENCY]
                                * self.battery_params[BatteryParameters.POWER_OUTPUT] * (self.battery_params[BatteryParameters.DELTA_T] / delta_t_sim))
                               / self.battery_params[BatteryParameters.NOMINAL_ENERGY])

        future_charge = current_charge + delta_change_charge
        minimum_charge = self.battery_params[BatteryParameters.SOC_MIN]
        maximum_charge = self.battery_params[BatteryParameters.SOC_MAX]

        if (future_charge >= minimum_charge) & (future_charge <= maximum_charge):
            self.battery_params[BatteryParameters.SOC_INI_ACTUAL] += delta_change_charge
            print(f"Battery changed!!! New SoC: {round(self.battery_params[BatteryParameters.SOC_INI_ACTUAL], 3)}")
        else:
            print("Battery limits exceeded (totally full or discharged), battery SoC stays the same.")


class BatteryMQTT(BatteryModule, mqtt.Client):
    """
    This class handles the subscription and publications of the battery module.
    """

    def __init__(self,
                 battery_id,
                 client_id_mqtt,
                 mqtt_server_ip,
                 mqtt_server_port,
                 enable_sma,
                 modbus_ip,
                 modbus_port):

        BatteryModule.__init__(self, id=battery_id, enable_sma=enable_sma, modbus_ip=modbus_ip, modbus_port=modbus_port)
        mqtt.Client.__init__(self, client_id=client_id_mqtt)

        self.topics = Topics

        # Subscribe
        self.topics.controller_set_battery_power_topic += f"battery_{battery_id}"
        self.topics.user_set_battery_parameters_topic += f"battery_{battery_id}"

        # Publish
        self.topics.battery_settings_topic += f"battery_{battery_id}"

        self.qos = 1  # QoS of the MQTT messages
        self.connect(host=mqtt_server_ip,
                     port=mqtt_server_port)

    def on_connect(self, mqtt, obj, flags, rc):
        print("Connected with result code " + str(rc))

        # Subscriber topics
        print(f"Subscribing to: {self.topics.controller_set_battery_power_topic}")
        self.subscribe(self.topics.controller_set_battery_power_topic, self.qos)

        # # Send the battery parameters to the controller for the first time
        # battery_parameters_dict = self.get_battery_parameters()
        # message_dict = json.dumps(battery_parameters_dict)
        # self.publish_response(topic=self.topics.battery_settings_topic, payload=message_dict)

        self.send_battery_parameters()

    def on_message(self, client, userdata, msg):
        is_command_processed = False
        print("--" * 100)
        print(bcolors.OKGREEN + f"Message received on topic: {msg.topic}" + bcolors.ENDC)
        # TODO: Check that all incomming messages are not empty

        if msg.topic == self.topics.controller_set_battery_power_topic:  # Received a new power set point
            print(bcolors.OKGREEN + "Receiving power set point" + bcolors.ENDC)
            received_message = msg.payload.decode('utf-8')

            if received_message:  # Set point of power from te controller has a time stamp
                received_message_frame = pd.read_json(received_message,
                                                      convert_dates=[ControlParameters.DATE_STAMP_OPTIMAL])
                received_message_frame = received_message_frame.set_index(ControlParameters.DATE_STAMP_OPTIMAL,
                                                                          drop=True)
                new_battery_power_set_point = received_message_frame[ControlParameters.BATTERY_POWER_OPTIMAL][0]
                self.set_power_output(new_battery_power_set_point)
                self.set_emergency_solutions(received_message_frame)
                is_command_processed = True
            else:
                print(bcolors.FAIL + "Empty message received" + bcolors.ENDC)
                is_command_processed = False

        elif msg.topic == self.topics.user_set_battery_parameters_topic:
            print(bcolors.OKGREEN + "Receiving new battery parameters from user" + bcolors.ENDC)
            received_message = msg.payload.decode('utf-8')

            if received_message:
                received_message_dict = json.loads(received_message)
                self.update_battery_parameters(**received_message_dict)
                self.publish_response(topic=self.topics.battery_settings_topic, payload=received_message_dict)
                is_command_processed = True
            else:
                print(bcolors.FAIL + "Empty message received" + bcolors.ENDC)
                is_command_processed = False

        if is_command_processed:
            print("All messages processed")
        else:
            print("Something went wrong")

    def publish_response(self, topic, payload):
        print(bcolors.OKBLUE + f"Publishing on topic: {topic}" + bcolors.ENDC)
        result_mqtt = self.publish(topic=topic, payload=payload)

        return result_mqtt

    def process_mqtt_messages(self):
        self.loop()

    # def simulate_heart_beat(self, delta_t_sim):
    #     self.simulate_battery_operation(delta_t_sim=delta_t_sim)  # Charge/Discharge the battery
    #     battery_parameters_dict = self.get_battery_parameters()
    #     message_dict = json.dumps(battery_parameters_dict)
    #     self.publish_response(topic=self.topics.battery_settings_topic,
    #                           payload=message_dict)

    def send_battery_parameters(self):
        battery_parameters_dict = self.get_battery_parameters()
        time_stamp = pd.to_datetime(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")).\
                                                    tz_localize("Europe/Amsterdam").tz_convert("UTC").isoformat(sep=' ')
        battery_parameters_dict.update({ControlParameters.DATE_STAMP_OPTIMAL: time_stamp})
        message_dict = json.dumps(battery_parameters_dict)
        self.publish_response(topic=self.topics.battery_settings_topic,  # Current battery settings to the controller (no time stamp/ no output power)
                              payload=message_dict)


    def virtual_battery_operation(self, delta_t_sim=1):
        """Reads the current battery status and report to the mosquitto broker"""
        if not self.enable_sma:  # Simulation mode
            self.simulate_battery_operation(delta_t_sim=delta_t_sim)  # Charge/Discharge the battery in simulation mode

        self.send_battery_parameters()
        # battery_parameters_dict = self.get_battery_parameters()
        # message_dict = json.dumps(battery_parameters_dict)
        # self.publish_response(topic=self.topics.battery_settings_topic,  # Current battery settings to the controller (no time stamp/ no output power)
        #                       payload=message_dict)


def scheduler_simulation_job(battery_instance):
    battery_instance.virtual_battery_operation()

def scheduler_report_battery_statys_job(battery_instance):
    battery_instance.send_battery_parameters()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-H', '--host', required=False, default="localhost")
    parser.add_argument('-P', '--port', required=False, type=int, default=1883,
                        help='8883 for TLS or 1883 for non-TLS')
    parser.add_argument('-c', '--clientid', required=False, default="BATTERY",
                        help="Client id for the mosquitto server")
    parser.add_argument('-B', '--batteryid', required=False, type=int, default=1,
                        help="Battery id (use integers). e.g., 1, 2, 3, etc.")
    parser.add_argument('--mode', required=False, type=int, default=0, choices={0, 1},
                        help="0: Enables simulation mode, 1: Use an actual battery to control.")
    parser.add_argument('--enableinverter', required=False, default=False, action='store_true',
                        help="False: Enables simulation mode (virtual battery), True: Real operation with modbus.")
    parser.add_argument('--ipmodbus', required=False, type=str, default="192.168.105.20",
                        help="IP address of the battery inverter.")
    parser.add_argument('--portmodbus', required=False, type=int, default=502,
                        help="Modbus port of the battery inverter.")
    parser.add_argument('--simdelay', required=False, type=float, default=0.2499,
                        help="Delay time for simulation operation.")
    parser.add_argument('--statusdelay', required=False, type=float, default=5,
                        help="Delay time for reporting the battery parameters/status.")
    parser.add_argument('--smatest', required=False, default=True, action='store_false',
                        help="Test the operation of the inverter via the docker container")

    args, unknown = parser.parse_known_args()

    print(f"ip address: {args.host}")
    print(f"Port: {args.port}")
    print(f"Client id: {args.clientid}")
    print(f"Battery id: {args.batteryid}")
    print(f"Enable inverter: {args.enableinverter}")
    print(f"Simulation every: {args.simdelay} seconds")
    print(f"Report battery parameters every: {args.statusdelay} seconds")

    battery = BatteryMQTT(battery_id=args.batteryid,
                          client_id_mqtt=args.clientid,
                          mqtt_server_ip=args.host,
                          mqtt_server_port=args.port,
                          enable_sma=args.enableinverter,
                          modbus_ip=args.ipmodbus,
                          modbus_port=args.portmodbus)

    if args.smatest:
        print("Setting the output power of the battery to 2000 [W] for 20 seconds...")
        battery.set_power_output(2.0)  # kW
        for ii in range(20):
            print(f"Second {ii + 1}/20...")
            time.sleep(1)
        print("Setting the output power of the battery to -2000 [W] for 20 seconds...")
        battery.set_power_output(-2.0)  # kW
        for ii in range(20):
            print(f"Second {ii + 1}/20...")
            time.sleep(1)
        print("Turning off the battery during 10 seconds...")
        battery.set_power_output(0.0)  #kW
        for ii in range(10):
            print(f"Second {ii + 1}/10...")
            time.sleep(1)


    if args.enableinverter and args.mode == 0:
        print(f"SMA enabled. BUT IN SIMULATION MODE! -- Sim delay: {args.simdelay} seconds")
        schedule.every(args.simdelay).seconds.do(scheduler_simulation_job, battery_instance=battery)
    elif args.enableinverter and args.mode == 1:
        # Real operation of the battery.
        print(f"SMA enabled. Controlling in real life!! -- Reporting battery status every: {args.statusdelay} seconds.")
        schedule.every(args.statusdelay).seconds.do(scheduler_report_battery_statys_job, battery_instance=battery)
    elif not args.enableinverter and args.mode == 0:
        print(f"SMA disabled (Simulation mode). Controlling a VIRTUAL battery!! -- Sim delay: {args.simdelay} seconds")
        schedule.every(args.simdelay).seconds.do(scheduler_simulation_job, battery_instance=battery)
        schedule.every(args.statusdelay).seconds.do(scheduler_report_battery_statys_job, battery_instance=battery)
    else:
        raise ValueError("Can not disable inverter and control the real battery at the same time. Check input arguments.")


    while True:
        battery.process_mqtt_messages()
        schedule.run_pending()