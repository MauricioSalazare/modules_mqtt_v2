from forecast_mqtt import ForecastIcarus
from control_mqtt import ControlModule
from battery_mqtt import BatteryModule
import pandas as pd
import numpy as np
from commons.parameters import BatteryParameters, ControlParameters
import matplotlib.pyplot as plt

# Create forecast
icarus = ForecastIcarus(id_sensor='gebouw')
controller = ControlModule()

#%%
PERFECT_PREDICTION = True
solutions_frame = list()
real_consumption = list()
forecast_consumption = list()


for simulation_time_step in range(len(icarus.join_time_series.index)-100):
    print("-" * 80)
    print(f"Simulation step: {simulation_time_step}" )
    print("-" * 80)
    print('*' * 200)

    if PERFECT_PREDICTION:
        (real_, prediction_) = icarus.get_prediction(simulation_time_step, 100)
        forecast_ = real_
    else:
        (real_, prediction_) = icarus.get_prediction(simulation_time_step, 100)
        forecast_ = prediction_


    # Simulate the send and receive of the forecast and controller via the MQTT class
    sent_message = forecast_.reset_index().to_json(date_format='iso')  # Sent by forecast
    received_message = pd.read_json(sent_message, convert_dates=['datetimeFC'])  # Received by Controller
    received_message = received_message.set_index('datetimeFC', drop=True)

    # Controller process the data
    controller.update_forecast(received_message)
    results_optimizer = controller.solve_model()
    print(f"Results optimizer: \n"
          f"{results_optimizer[[ControlParameters.DATE_STAMP_OPTIMAL, ControlParameters.BATTERY_POWER_OPTIMAL, ControlParameters.SOC_BATTERY_OPTIMAL]].head()}")

    if simulation_time_step == 0:
        print('X' * 200)
        print('Battery connected')
        print('X' * 200)
        # Battery connected
        battery = BatteryModule(id=1)
        controller.update_battery_parameters(**battery.get_battery_parameters())

    # if simulation_time_step == 50:
    #     print("Use changed the Power set point")
    #     change_max_power={ControlParameters.POWER_THRESHOLD: 7.5}
    #     controller.update_controller_parameters(**change_max_power)

    # if simulation_time_step == 100:
    #     print("Use changed the Power set point")
    #     change_max_power={ControlParameters.POWER_THRESHOLD: 7.5}
    #     controller.update_controller_parameters(**change_max_power)

    # if simulation_time_step == 200:
    #     print("Use changed the Power set point")
    #     change_max_power={ControlParameters.POWER_THRESHOLD: 5.0}
    #     controller.update_controller_parameters(**change_max_power)

    # Simulate send and receive of the controller to te battery via the MQTT class
    sent_message_to_battery = results_optimizer[['datetimeFC', 'p_battery']].to_json(date_format='iso', orient='records')
    received_message_from_controller = pd.read_json(sent_message_to_battery, convert_dates=['datetimeFC'])
    received_message_from_controller = received_message_from_controller.set_index('datetimeFC', drop=True)

    # Set new power output on the battery
    if simulation_time_step != 0:
        battery.set_power_output(received_message_from_controller['p_battery'].iloc[0])

        # Simulate battery charge/discharge
        for ii in range(4):
            battery.simulate_battery_operation(delta_t_sim=4)

    # Send the new status of the SoC
    controller.update_battery_parameters(**{'soc_ini': battery.battery_params['soc_ini']})

    solutions_frame.append(results_optimizer.iloc[0,:])
    real_consumption.append(real_[0])
    forecast_consumption.append(forecast_[0])

solutions_frame = pd.concat(solutions_frame, axis=1).transpose().set_index('datetimeFC', drop=True)
real_consumption = np.array(real_consumption)
forecast_consumption = np.array(forecast_consumption)


#%%
#############################################################################################################
#############################################################################################################
#############################################################################################################
################################# PLOTTING RESULTS  #########################################################
#############################################################################################################
#############################################################################################################
#############################################################################################################
#############################################################################################################


net_power = (real_consumption
            + solutions_frame[ControlParameters.BATTERY_POWER_OPTIMAL].values)

x_ = solutions_frame.index
fig = plt.figure(figsize=(12, 7))
gs = fig.add_gridspec(4, 1, hspace=0.5)
ax1 = fig.add_subplot(gs[0:2, :])
ax2 = fig.add_subplot(gs[2, :])
ax3 = fig.add_subplot(gs[3, :])

delta_building_consumption = (real_consumption
                              - solutions_frame[ControlParameters.POWER_THRESHOLD].values)
delta_net_demand = net_power - solutions_frame[ControlParameters.POWER_THRESHOLD].values

delta_building_consumption[delta_building_consumption < 0] = 0
delta_net_demand[delta_net_demand < 0] = 0

ax1.step(x_, solutions_frame[ControlParameters.BATTERY_POWER_OPTIMAL].values, label='Battery demand', color='b')
ax1.step(x_, real_consumption, label='Building real consumption', color='orange')
ax1.step(x_, forecast_consumption, label='Building predicted consumption', color='g')
ax1.step(x_, net_power, label='Net demand real', color='r')

ax1.step(x_, solutions_frame[ControlParameters.POWER_THRESHOLD].values, label='Power Threshold', linestyle='--',
         linewidth=0.3, color='g')
ax1.axhline(0, linestyle='--', linewidth=0.3, color='b')
ax1.legend(loc='upper center', bbox_to_anchor=(0.5, 1.2), ncol=5)


ax2.step(x_, delta_building_consumption, label='Delta building consumption', color='orange')
ax2.step(x_, delta_net_demand, label='Delta net demand', color='r')
ax2.legend(loc='upper center', fontsize=7, ncol=2)
ax2.set_ylabel('[kW]')


ax3.step(x_, solutions_frame[BatteryParameters.SOC_INI_ACTUAL].values, color='darkblue', label='State of charge')
ax3.step(x_, solutions_frame[BatteryParameters.SOC_MIN].values, linestyle='--', color='darkblue', linewidth=0.3)
ax3.step(x_, solutions_frame[BatteryParameters.SOC_MAX].values, linestyle='-', color='darkblue', linewidth=0.3)
ax3.set_ylim([-0.1, 1.1])
ax3.legend(loc='lower right', fontsize=7)
ax3.set_ylabel('[p.u]')
