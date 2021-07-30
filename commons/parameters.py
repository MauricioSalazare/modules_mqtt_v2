class BatteryParameters:
    NOMINAL_ENERGY = 'pb_nom'  # Nominal power of the battery in kWh
    MAX_POWER_DISCHARGE = 'pb_discharge_max' # Max. power to discharge in kW
    MAX_POWER_CHARGE = 'pb_charge_max'
    DELTA_T = 'delta_t'
    SOC_MIN = 'soc_min'
    SOC_MAX = 'soc_max'
    EFFICIENCY = 'efficiency'
    SOC_INI_ACTUAL = 'soc_ini'
    POWER_OUTPUT = 'pb_actual_power'
    VALID_KEYS = [NOMINAL_ENERGY,
                  MAX_POWER_DISCHARGE,
                  MAX_POWER_CHARGE,
                  DELTA_T,
                  SOC_MIN,
                  SOC_MAX,
                  EFFICIENCY,
                  SOC_INI_ACTUAL,
                  POWER_OUTPUT]


class ControlParameters:
    # Solutions from the controllers
    BATTERY_POWER_OPTIMAL = 'p_battery'
    NET_POWER_OPTIMAL = 'p_net_demand'
    SOC_BATTERY_OPTIMAL = 'soc_battery'
    DATE_STAMP_OPTIMAL = 'datetimeFC'
    FORECAST_VALUES = 'forecast'

    # Settings of the controller
    POWER_THRESHOLD = 'p_net_threshold'
    OPTIMIZER_WINDOW = 'mpc_window'

    VALID_KEYS = [POWER_THRESHOLD,
                  OPTIMIZER_WINDOW]


class Topics:
    sensor_topic                       = r"sensor/active_power/"
    forecast_topic                     = r"forecast/active_power/"
    forecast_stop_simulation           = r"forecast/stop_simulation_command"
    battery_settings_topic             = r"battery/battery_settings/"
    user_control_setting_topic         = r"user/controller/control_settings/"
    user_set_model_topic               = r"user/controller/set_model/"
    user_set_battery_parameters_topic  = r"user/battery/set_battery_parameters/"
    controller_results                 = r"controller/optimizer_results/"
    # controller_to_battery_topic        = r"controller/set_power_battery/"
    controller_settings_response       = r"controller/settings_response/"
    controller_set_battery_power_topic = r"controller/set_power_battery/"



class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
