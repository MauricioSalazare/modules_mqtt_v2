import paho.mqtt.client as mqtt
from pyomo.environ import *
import pandas as pd
import json
import numpy as np
import argparse
from commons.parameters import BatteryParameters, ControlParameters, bcolors, Topics
import os


class ControlModule:
    """
    Control module based on an MPC optimizer.
    """

    def __init__(self):
        """ Default battery settings that will be used for the optimizer (This avoids the optimizer to crash)
         This dictionary will be updated by the battery module constantly.
         The following parameters are the minimum required to do the optimization. Therefore, this is a "copy" of the
         parameters that are in the battery module"""
        self.battery_params = {BatteryParameters.NOMINAL_ENERGY: 30,
                               BatteryParameters.MAX_POWER_DISCHARGE: 6,
                               BatteryParameters.MAX_POWER_CHARGE: 6,
                               BatteryParameters.DELTA_T: 0.25,
                               BatteryParameters.SOC_MIN: 0.2,
                               BatteryParameters.SOC_MAX: 1.0,
                               BatteryParameters.EFFICIENCY: 1.0,
                               BatteryParameters.SOC_INI_ACTUAL: 0.8}

        # Default controller settings: (This data should come from the user module)
        self.controller_params = {ControlParameters.POWER_THRESHOLD: 5, # kW
                                  ControlParameters.OPTIMIZER_WINDOW: 96}

        self.power_threshold = 6.5  # Maximum power allowed in the phase

        # Default forecast values: (This should come from the forecasting module)
        self.forecast = dict(zip(range(self.controller_params[ControlParameters.OPTIMIZER_WINDOW]),
                                 np.zeros(self.controller_params[ControlParameters.OPTIMIZER_WINDOW])))
        self.time_stamps_forecast = None
        self.model = None
        self.update_optimization_model()

        # Internal controller state variables: (Some could come from the user module).
        self.battery_on_line = False

    def update_forecast(self, Pd):
        assert isinstance(Pd, pd.DataFrame), "Forecast should be a pandas Data Frame with timestamp"
        power_values = Pd.values.ravel()

        self.time_stamps_forecast = Pd.index[:self.controller_params[ControlParameters.OPTIMIZER_WINDOW]]
        self.forecast = dict(zip(range(self.controller_params[ControlParameters.OPTIMIZER_WINDOW]),
                                 power_values[:self.controller_params[ControlParameters.OPTIMIZER_WINDOW]]))
        self.update_optimization_model()
        print("Forecast and model updated")

    def update_battery_parameters(self, **kwargs):
        if set(kwargs.keys()).issubset(BatteryParameters.VALID_KEYS):
            self.battery_params.update(kwargs)
            print(f"BATTERY Parameters updated: {kwargs}")
            print(f"SoC_ini in the controller: {round(self.battery_params[BatteryParameters.SOC_INI_ACTUAL], 3)}")

        else:
            print(bcolors.FAIL + "One of the dictionary keys of BATTERY parameters not valid" + bcolors.ENDC)
        self.update_optimization_model()

    def update_controller_parameters(self, **kwargs):
        if set(kwargs.keys()).issubset(ControlParameters.VALID_KEYS):
            self.controller_params.update(kwargs)
            print(f"CONTROLLER Parameters updated: {kwargs}")
            print(f"Power Threshold in the controller: {self.controller_params[ControlParameters.POWER_THRESHOLD]}")
        else:
            print(bcolors.FAIL + "One of the dictionary keys of CONTROLLER parameters is not valid" + bcolors.ENDC)
        self.update_optimization_model()

    def update_optimization_window(self, new_window):
        print(f"New window: {int(new_window)}")
        self.controller_params[ControlParameters.OPTIMIZER_WINDOW] = int(new_window)
        self.update_optimization_model()

    def update_power_threshold(self, new_power_threshold):
        self.controller_params[ControlParameters.POWER_THRESHOLD] = new_power_threshold
        self.update_optimization_model()

    def update_optimization_model(self):
        self.model = ControlModule.mpc_model(self.forecast,
                                             self.controller_params[ControlParameters.POWER_THRESHOLD],
                                             **self.battery_params)

    def solve_model(self):
        solver = SolverFactory('ipopt')
        results = solver.solve(self.model, tee=False)

        if (results.solver.status == SolverStatus.ok) and (
                results.solver.termination_condition == TerminationCondition.optimal):
            print('Found optimal solution')
            print(f'-- SoC initial: {self.battery_params[BatteryParameters.SOC_INI_ACTUAL]}')

        elif results.solver.termination_condition == TerminationCondition.infeasible:
            print('Solution Infeasible')
            print(f'-- SoC initial: {self.battery_params[BatteryParameters.SOC_INI_ACTUAL]}')
        else:
            print('Something went wrong')

        # Built optimal results data frame, adding the parameters used for the optimization
        results_dict = {ControlParameters.DATE_STAMP_OPTIMAL: self.time_stamps_forecast,
                        ControlParameters.FORECAST_VALUES: np.array(list(self.forecast.values())),
                        ControlParameters.BATTERY_POWER_OPTIMAL:
                            np.array([self.model.Pb[t].value
                                      for t in np.arange(self.controller_params[ControlParameters.OPTIMIZER_WINDOW])]),
                        ControlParameters.NET_POWER_OPTIMAL:
                            np.array([self.model.Pn[t].value
                                      for t in np.arange(self.controller_params[ControlParameters.OPTIMIZER_WINDOW])]),
                        ControlParameters.SOC_BATTERY_OPTIMAL:
                            np.array([self.model.SoCb[t].value
                                      for t in np.arange(self.controller_params[ControlParameters.OPTIMIZER_WINDOW])])}

        battery_parameters = dict()
        for (key_, value_) in self.battery_params.items():
            battery_parameters.setdefault(key_, np.repeat(value_,
                                                          self.controller_params[ControlParameters.OPTIMIZER_WINDOW]))

        controller_parameters = dict()
        for (key_, value_) in self.controller_params.items():
            controller_parameters.setdefault(key_,
                                             np.repeat(value_,
                                                       self.controller_params[ControlParameters.OPTIMIZER_WINDOW]))

        results_dict.update(battery_parameters)
        results_dict.update(controller_parameters)
        results_optimizer = pd.DataFrame(results_dict)

        return results_optimizer

    @staticmethod
    def mpc_model(Pd, Pn_max, **kwargs):
        # Set default parameters to avoid a crash in the optimizer
        Pbnom = kwargs.setdefault(BatteryParameters.NOMINAL_ENERGY, 15)
        Pb_discharg_max = kwargs.setdefault(BatteryParameters.MAX_POWER_DISCHARGE, 6)
        Pb_charg_max = kwargs.setdefault(BatteryParameters.MAX_POWER_CHARGE, 6)
        Delta_t = kwargs.setdefault(BatteryParameters.DELTA_T, 0.25)
        SoCmin = kwargs.setdefault(BatteryParameters.SOC_MIN, 0.2)
        SoCmax = kwargs.setdefault(BatteryParameters.SOC_MAX, 1.0)
        n_b = kwargs.setdefault(BatteryParameters.EFFICIENCY, 1.0)
        SoCini = kwargs.setdefault(BatteryParameters.SOC_INI_ACTUAL, 0.8)
        T = list(Pd.keys())

        # Type of Model
        model = ConcreteModel()

        # Sets
        model.T = Set(initialize=T)

        # Parameters
        model.Pd = Param(model.T, initialize=Pd, mutable=True)
        model.n_b = Param(initialize=n_b, mutable=True)
        model.SoCini = Param(initialize=SoCini, mutable=True)
        model.SoCmin = Param(initialize=SoCmin, mutable=True)
        model.SoCmax = Param(initialize=SoCmax, mutable=True)
        model.Delta_t = Param(initialize=Delta_t, mutable=True)
        model.Pbnom = Param(initialize=Pbnom, mutable=True)
        model.Pb_charg_max = Param(initialize=Pb_charg_max, mutable=True)
        model.Pb_discharg_max = Param(initialize=Pb_discharg_max, mutable=True)
        model.Pn_max = Param(initialize=Pn_max, mutable=True)

        # Variables
        model.Pn = Var(model.T, initialize=0.0)
        model.Pb = Var(model.T, initialize=0.0)
        model.SoCb = Var(model.T, initialize=SoCini)

        # -----------    Objective Function  ----------------------------------
        def min_net_power(model):
            return sum(((model.Pn[t] - model.Pn_max) ** 2) for t in model.T)

        model.obj = Objective(rule=min_net_power)

        # -----------    Constraints  -----------------------------------------
        def define_net_power_rule(model, t):
            return (model.Pn[t] == model.Pd[t] + model.Pb[t])

        model.define_net_power = Constraint(model.T, rule=define_net_power_rule)

        def define_state_of_charge_battery_rule(model, t):
            if t == 0:
                return (model.SoCb[t] == model.SoCini)
            else:
                return (model.SoCb[t] == model.SoCb[t - 1] + (n_b * model.Pb[t - 1] * Delta_t) / (model.Pbnom))

        model.define_state_of_charge_battery = Constraint(model.T, rule=define_state_of_charge_battery_rule)

        def define_limits_soc_min_rule(model, t):
            return (model.SoCmin <= model.SoCb[t])

        model.define_limits_soc_min = Constraint(model.T, rule=define_limits_soc_min_rule)

        def define_limits_soc_max_rule(model, t):
            return (model.SoCb[t] <= model.SoCmax)

        model.define_limits_soc_max = Constraint(model.T, rule=define_limits_soc_max_rule)

        def define_limits_dischar_rule(model, t):
            return (- model.Pb_discharg_max <= model.Pb[t])

        model.define_limits_dischar = Constraint(model.T, rule=define_limits_dischar_rule)

        def define_limits_charg_rule(model, t):
            return (model.Pb[t] <= model.Pb_charg_max)

        model.define_limits_charg = Constraint(model.T, rule=define_limits_charg_rule)

        return model


class ControlMQTT(ControlModule, mqtt.Client):
    """
    This class handles the subscription and publications of the control module.
    The module checks the message and executes the control and/or updates accordingly
    """

    def __init__(self,
                 control_id,
                 controlled_sensor_id,
                 controlled_phase_id,
                 battery_id,
                 client_id_mqtt,
                 mqtt_server_ip,
                 mqtt_server_port):

        ControlModule.__init__(self)
        mqtt.Client.__init__(self, client_id=client_id_mqtt + "_" + str(control_id))

        self.control_id = control_id
        self.controlled_sensor_id = controlled_sensor_id
        self.controlled_phase_id = controlled_phase_id
        self.battery_id = battery_id
        self.battery_on_line = False

        self.topics = Topics

        # Subscribe
        self.topics.forecast_topic += f"{controlled_sensor_id}_" + f"{controlled_phase_id}"
        self.topics.battery_settings_topic += f"battery_{battery_id}"
        self.topics.user_control_setting_topic += f"control_{control_id}"
        self.topics.user_set_model_topic += f"control_{control_id}"

        # Publish
        self.topics.controller_settings_response += f"control_{control_id}"
        self.topics.controller_set_battery_power_topic += f"battery_{battery_id}"
        self.topics.controller_results += f"control_{control_id}"

        self.qos = 1
        self.connect(host=mqtt_server_ip,
                     port=mqtt_server_port)

    def on_connect(self, mqtt, obj, flags, rc):
        print("Connected with result code " + str(rc))

        # Subscriber topics
        print(f"Subscribing to: {self.topics.forecast_topic}")
        self.subscribe(self.topics.forecast_topic, self.qos)

        print(f"Subscribing to: {self.topics.battery_settings_topic}")
        self.subscribe(self.topics.battery_settings_topic, self.qos)

        print(f"Subscribing to: {self.topics.user_control_setting_topic}")
        self.subscribe(self.topics.user_control_setting_topic, self.qos)

        print(f"Subscribing to: {self.topics.user_set_model_topic}")
        self.subscribe(self.topics.user_set_model_topic, self.qos)

    def on_message(self, client, userdata, msg):
        is_command_processed = False
        print("--" * 100)
        print(bcolors.OKGREEN + f"Message received on topic: {msg.topic}" + bcolors.ENDC)

        if msg.topic == self.topics.forecast_topic:  # Received a Forecast
            print(bcolors.OKGREEN + "Receiving forecast" + bcolors.ENDC)
            received_message = msg.payload.decode('utf-8')
            received_message_frame = pd.read_json(received_message,
                                                  convert_dates=[ControlParameters.DATE_STAMP_OPTIMAL])
            received_message_frame = received_message_frame.set_index(ControlParameters.DATE_STAMP_OPTIMAL, drop=True)

            self.update_forecast(received_message_frame)
            results_optimizer = self.solve_model()
            message = results_optimizer.to_json(date_format='iso')
            self.publish_response(topic=self.topics.controller_results, payload=message)  # For the DB Manager module

            if self.battery_on_line:
                print("Sending the new battery output power...")
                message = results_optimizer[[ControlParameters.DATE_STAMP_OPTIMAL,
                                             ControlParameters.BATTERY_POWER_OPTIMAL]].to_json(date_format='iso',
                                                                                               orient='records')
                # For the battery module
                self.publish_response(topic=self.topics.controller_set_battery_power_topic, payload=message)
            else:
                print("Battery is off-line... THE OUTPUT POWER WAS NOT SET")

            is_command_processed = True

        elif msg.topic == self.topics.battery_settings_topic:  # Received new battery settings DO NOT SOLVE ANYTHING
            print(bcolors.WARNING + "Receiving battery settings" + bcolors.ENDC)
            received_message = msg.payload.decode('utf-8')
            received_message_dict = json.loads(received_message)
            self.update_battery_parameters(**received_message_dict)

            if not self.battery_on_line:  # Battery send updates for the first time
                self.battery_on_line = True
                print("**** BATTERY ON-LINE ****")

            is_command_processed = True

        elif msg.topic == self.topics.user_control_setting_topic:  # New controller settings DO NOT SOLVE ANYTHING
            received_message = msg.payload.decode('utf-8')
            received_message_dict = json.loads(received_message)
            try:
                assert isinstance(received_message, dict), "Controller settings should be a dictionary"
                self.update_controller_parameters(**received_message_dict)
                is_command_processed = True
            except AssertionError:
                print("Battery settings was not a dictionary, UPDATE ABORTED")
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-H', '--host', required=False, type=str,  default='localhost')
    parser.add_argument('-P', '--port', required=False, type=int, default=1883,
                        help='8883 for TLS or 1883 for non-TLS')
    parser.add_argument('-c', '--clientid', required=False, default="CONTROL_1",
                        help="Client id for the mosquitto server")
    parser.add_argument('-C', '--controlid', required=False, type=int, default=1,
                        help="Control id for the MQTT topic. e.g., 1, 2, 3, etc.")
    parser.add_argument('-B', '--batteryid', required=False, type=int, default=1,
                        help="Battery id for the MQTT topic. e.g., 1, 2, 3, etc.")
    parser.add_argument('-S', '--sensorid', required=False, default='gebouw',
                        help="Name of the sensor measurement")
    parser.add_argument('-L', '--phaseid', required=False, default='l1',
                        help="Phase of the sensor measurement e.g., 'l1', 'l2' or 'l3'")

    args, unknown = parser.parse_known_args()

    print("Environment variables:")
    print(os.environ["PATH"])
    print(f"ip address: {args.host}")
    print(f"Port: {args.port}")
    print(f"Client id: {args.clientid}")
    print(f"Control id: {args.controlid}")
    print(f"Battery id: {args.batteryid}")
    print(f"Sensor id: {args.sensorid}")
    print(f"Phase id: {args.phaseid}")

    power_controller_mqtt = ControlMQTT(control_id=args.controlid,
                                        controlled_sensor_id=args.sensorid,
                                        controlled_phase_id=args.phaseid,
                                        battery_id=args.batteryid,
                                        client_id_mqtt=args.clientid,
                                        mqtt_server_ip=args.host,
                                        mqtt_server_port=args.port)
    # while True:
    #     power_controller_mqtt.process_mqtt_messages()
