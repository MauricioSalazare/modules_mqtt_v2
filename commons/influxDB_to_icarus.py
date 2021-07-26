from influxdb import InfluxDBClient  # Calling the InfluxDB Client
import pandas as pd
import requests
import json
import datetime
from pathlib import Path


class influxToIcarus:

    def __init__(self,
                 influx_host,
                 influx_port,
                 influx_user,
                 influx_password,
                 icarus_user,
                 icarus_password,
                 icarus_api_key,
                 sand_box=True):

        self.abspath = Path(__file__).parent

        self.influx_host = influx_host
        self.influx_port = influx_port
        self.influx_user = influx_user
        self.influx_password = influx_password

        self.icarus_user = icarus_user
        self.icarus_password = icarus_password
        self.icarus_api_key = icarus_api_key

        if sand_box:
            self.icarus_user = 'Icarusaccswaggerui'
            self.icarus_password = 'w9pabMkxqaO33MPk'
            self.icarus_api_key = 'test'

            self.icarus_url_measurement_controller = 'https://acc-data.icarus.energy/api/measurements'
            self.icarus_url_forecast = 'https://acc-data.icarus.energy/api/forecast'
        else:
            self.icarus_url_measurement_controller = 'https://data.icarus.energy/api/measurements'
            self.icarus_url_forecast = 'https://data.icarus.energy/api/forecast'

        self.influx_client = InfluxDBClient(self.influx_host, self.influx_port, self.influx_user, self.influx_password)
        print(f'InfluxDB ping response: {self.influx_client.ping()}')
        self.influx_client.switch_database('elaadnl_measurements')


    def get_influx_data(self, id_sensor, date_init, date_end, phase=None, in_kw=False):
        '''
        This is a debug method to check the connection to retrieve data,
        This is not used by the class for operation
        '''
        if phase is not None:
            if isinstance(phase, str):
                assert phase in ['l1', 'l2', 'l3'], 'Incorrect phase description'
                phases = [phase]
            elif isinstance(phase, list):
                assert set(phase).issubset(['l1', 'l2', 'l3']), 'Incorrect phase description'
                phases = phase
        else: # All phases are used
            phases = ['l1', 'l2', 'l3']

        transaction_ok = list()
        data_frame_response = list()

        for phase_sensor in phases:
            print('=' * 70)
            print(f'Sensor: {id_sensor}')
            print(f'Phase: {phase_sensor}')
            # Get the data from the influxDB
            # Assuming that all values of active power < 0.0 are wrong values
            result = self.influx_client.query(f'''SELECT * FROM (
                   	SELECT mean("active_power") from "elaad_testplein" where
                      	    ("id"= '{id_sensor}' and "phase" = '{phase_sensor}' 
                      	    and time >= '{date_init}' and time < '{date_end}' 
                      	    and "active_power" > 0.0) GROUP BY time(15m))''')

            self.result_posting = result

            if len(result) == 0:
                print('No response from the SQL query. Aborting')
                return False

            data_frame = pd.DataFrame(list(result.get_points('elaad_testplein')))
            data_frame.time = pd.to_datetime(data_frame.time, utc=True)
            data_frame.time = data_frame.time.dt.strftime('%Y-%m-%dT%H:%M:%S%z')
            data_frame.columns = ['datetime', 'output']

            data_frame_response.append(data_frame)

        data_frame_response = [frame_data.set_index('datetime') for frame_data in data_frame_response]
        data_frame_response = pd.concat(data_frame_response, axis=1)
        if in_kw:
            data_frame_response = data_frame_response.divide(1000.)  # Convert to kW
        data_frame_response.columns = phases
        data_frame_response.index.name = 'datetimeFC'
        data_frame_response.index = pd.to_datetime(data_frame_response.index)

        return data_frame_response


    def post_sensor_data(self, id_sensor, date_init, date_end):

        assert date_init != date_end, 'Initial and final dates should be different'
        assert isinstance(date_init, str), 'Date must be a string with format YYYY-MM-DDThh:mm:ssZ'
        assert isinstance(date_end, str), 'Date must be a string with format YYYY-MM-DDThh:mm:ssZ'

        phases = ['l1', 'l2', 'l3']
        transaction_ok = list()

        for phase_sensor in phases:
            print('=' * 70)
            print(f'Sensor: {id_sensor}')
            print(f'Phase: {phase_sensor}')
            # Get the data from the influxDB
            # Assuming that all values of active power < 0.0 are wrong values
            result = self.influx_client.query(f'''SELECT * FROM (
            	SELECT mean("active_power") from "elaad_testplein" where
               	    ("id"= '{id_sensor}' and "phase" = '{phase_sensor}' 
               	    and time >= '{date_init}' and time < '{date_end}' 
               	    and "active_power" > 0.0) GROUP BY time(15m))''')

            self.result_posting = result

            if len(result) == 0:
                print('No response from the SQL query. Aborting')
                return False

            data_frame = pd.DataFrame(list(result.get_points('elaad_testplein')))
            data_frame.time = pd.to_datetime(data_frame.time, utc=True)
            data_frame.time = data_frame.time.dt.strftime('%Y-%m-%dT%H:%M:%S%z')
            data_frame.columns = ['datetime', 'output']

            self.data_frame = data_frame

            init_datetime = pd.to_datetime(data_frame.datetime.iloc[0]).strftime('%Y-%m-%dT%H:%M:%S') + 'Z'
            end_datetime = pd.to_datetime(data_frame.datetime.iloc[-1]).strftime('%Y-%m-%dT%H:%M:%S') + 'Z'

            # Create the payload for iCarus:
            head = '{"data":'
            tail = f',"sid": "{id_sensor}_{phase_sensor}_"}}'  # The extra } is to "Escape" the braces

            payload = data_frame.to_json(orient='records', date_format='iso')
            payload_frame = head + payload + tail
            payload_json = json.loads(payload_frame)

            ans = requests.post(self.icarus_url_measurement_controller,
                                params={'key': self.icarus_api_key},
                                json=payload_json,
                                auth=(self.icarus_user, self.icarus_password))

            print(f'Status code: {ans.status_code}')
            print(f'Ok answer: {ans.ok}')
            # print('Headers: ')
            # print(ans.headers)
            transaction_ok.append(ans.ok)

        return all(transaction_ok), init_datetime, end_datetime


    def update_icarus_data(self, id_sensor):
        print('=' * 70)
        print('Updating the icarus database from the influxDB')
        print(f'Sensor: {id_sensor}')

        if Path.is_file(Path(__file__).parent / 'back_log.txt'):
            print('BACK LOG file exists. Loading back_log.txt ...')
            back_log = pd.read_csv('back_log.txt')
            print('Back log table:')
            print(back_log)
            print('\n')
            print('Requesting to update the database from the following ranges...')

            date_init = ( pd.to_datetime(back_log.final_date.iloc[-1]) +
                                        pd.Timedelta(minutes=15) ).strftime('%Y-%m-%dT%H:%M:%S') + 'Z'
            date_now = datetime.datetime.now()

            # Round the current time to the nearest 15 minutes quarter of the day.
            date_now = datetime.datetime(date_now.year, date_now.month, date_now.day, date_now.hour,
                                         15 * (date_now.minute // 15))
            date_end = pd.to_datetime(date_now)
            date_end = date_end.strftime('%Y-%m-%dT%H:%M:%S') + 'Z'  # The extra 'Z' is for the format in influxDB

            print(f'Initial date: {date_init}')
            print(f'Final date: {date_end}')

            transaction_ok, date_init, date_end = self.post_sensor_data(id_sensor=id_sensor,
                                                                        date_init=date_init,
                                                                        date_end=date_end)

            if transaction_ok:
                print('*' * 35)
                print('Range updated in Icarus')
                print(f'Initial date: {date_init}')
                print(f'Final date: {date_end}')

                date_now_back_log = pd.to_datetime(datetime.datetime.now())
                date_now_back_log = date_now_back_log.strftime('%Y-%m-%dT%H:%M:%S')

                back_log_update = pd.DataFrame.from_dict({'initial_date': [date_init],
                                                          'final_date': [date_end],
                                                          'update_time': [date_now_back_log]})
                back_log = pd.concat([back_log, back_log_update], ignore_index=True)
                back_log.to_csv('back_log.txt', index=False)
                print('Back log updated... OK!')
                return True

            else:
                print('The data WAS NOT update on icarus')
                return False

        else:
            '''The end date day is not include in the dataset, the range is not inclusive in the right limit'''

            # Create the back_log from the first time.
            print('BACK LOG file does not exist. Creating a new back_log.txt ...')
            print('Requesting to update the database from the following ranges...')
            date_init = '2020-01-01T00:00:00Z'
            date_end = '2020-09-05T00:00:00Z'  # This could be any date, it is just the first update

            print(f'Initial date: {date_init}')
            print(f'Final date: {date_end}')

            transaction_ok, date_init, date_end = self.post_sensor_data(id_sensor=id_sensor,
                                                                        date_init=date_init,
                                                                        date_end=date_end)

            if transaction_ok:
                print('*' * 35)
                print('Range updated in Icarus')
                print(f'Initial date: {date_init}')
                print(f'Final date: {date_end}')

                date_now_back_log = pd.to_datetime(datetime.datetime.now())
                date_now_back_log = date_now_back_log.strftime('%Y-%m-%dT%H:%M:%S')

                pd.DataFrame.from_dict({'initial_date': [date_init],
                                        'final_date': [date_end],
                                        'update_time': [date_now_back_log]}).to_csv('back_log.txt', index=False)
            else:
                print('The data WAS NOT update on icarus AND the backlog WAS NOT created')
                return False


    def get_icarus_forecast(self, from_date, phase=None, days=7, include_components=False, in_kw=False):
        if include_components:
            include_components_flag = 'true'
        else:
            include_components_flag = 'false'

        payload = {'days': days,
                   'fromDate': from_date,
                   'includeComponents': include_components_flag,
                   'key': self.icarus_api_key}

        ans = requests.get(self.icarus_url_forecast,
                           params=payload,
                           auth=(self.icarus_user, self.icarus_password))

        print(f'Status code: {ans.status_code}')
        print(f'Ok answer: {ans.ok}')
        self.ans = ans

        if ans.ok:
            if phase is None:
                phases = range(3)
            else:
                assert isinstance(phase, str), 'Input should be the "l1", "l2" or "l3"'
                assert phase in ['l1', 'l2', 'l3'], 'Incorrect phase descriptor. i.e. ''l1'', ''l2'' or ''l3'''
                phases_dict = {'l1': [0],
                               'l2': [1],
                               'l3': [2]}
                phases = phases_dict[phase]

            forecast_frame = list()
            for phase_id in phases:
                get_data = json.loads(ans.content.decode('utf-8'))[phase_id]
                get_data_frame = pd.DataFrame(get_data['forecasts'])
                get_data_frame.datetimeFC = pd.to_datetime(get_data_frame.datetimeFC, utc=True)
                get_data_frame.set_index('datetimeFC', inplace=True)
                get_data_frame = get_data_frame.tz_convert('UTC')
                get_data_frame.reset_index(inplace=True)
                get_data_frame.rename(columns={'forecast': 'forecast_l' + str(phase_id + 1)}, inplace=True)

                # I am dropping all columns, except the forecast
                # I am also assuming that all phases has the same time stamp so I can concatenate them
                forecast_frame.append(get_data_frame[['datetimeFC',
                                                      'forecast_l' + str(phase_id + 1)]].set_index('datetimeFC',
                                                                                                   drop=True))

            forecast_frame = pd.concat(forecast_frame, axis=1)

            if in_kw:
                forecast_frame = forecast_frame.divide(1000)

            return forecast_frame

        else:
            print('Something went wrong')
            return None


    def get_icarus_range_data(self, id_sensor, date_init, date_end):
        # import pandas as pd
        # date_init = '2020-02-07'
        # date_end = '2020-02-10'
        # id_sensor = 'gebouw'

        init_date = pd.to_datetime(date_init)
        end_date = pd.to_datetime(date_end)

        assert (init_date < end_date), 'Start date should be greater than end date'

        daily_date_range = pd.date_range(init_date, end_date, freq='d')


        phases = ['l1', 'l2', 'l3']

        big_frames_list = list()
        for phase in phases:
            frames_list = list()
            for day_date in daily_date_range:
                print(day_date.strftime('%Y-%m-%d'))
                data_frame = self.get_icarus_data(id_sensor=id_sensor, phase=phase, date=day_date.strftime('%Y-%m-%d'))
                # data_frame.set_index('datetime', inplace=True)
                frames_list.append(data_frame)

            #TODO: Here I am assuming that all the phases has the same time stamps

            # frames_concat = pd.concat(frames_list).reset_index(drop=True)
            frames_concat = pd.concat(frames_list)
            frames_concat.rename(columns={'output': phase}, inplace=True)

            big_frames_list.append(frames_concat)

        big_frame_concat = pd.concat(big_frames_list, axis=1)

        return big_frame_concat


    def get_icarus_data(self, id_sensor, date, phase='l1'):

        #TODO: What happens if there is no data in the date selected? How to deal with this?

        assert isinstance(date, str), 'Date must be a string with format YYYY-MM-DD'

        print('=' * 70)
        print('Retrieving influxDB data saved on Icarus')
        print(f'Sensor: {id_sensor}')
        print(f'Phase: {phase}')
        print(f'Date: {date}')

        pay_load = {'date': date,
                    'key': self.icarus_api_key,
                    'sid': f'{id_sensor}_{phase}_'}

        ans = requests.get(self.icarus_url_measurement_controller,
                           params=pay_load,
                           auth=(self.icarus_user, self.icarus_password))

        print(f'Status code: {ans.status_code}')
        print(f'Ok answer: {ans.ok}')

        self.ans_get = ans

        if not ans.ok:
            print('Something went wrong')
            return None

        get_data = json.loads(ans.content.decode('utf-8'))
        get_data_frame = pd.DataFrame(get_data['data'])

        if get_data_frame.empty:
            print(f"Data frame empty -- date: {date} -- id_sensor: {id_sensor} -- phase: {phase}")
            return get_data_frame

        get_data_frame.datetime = pd.to_datetime(get_data_frame.datetime, utc=True)
        get_data_frame.set_index('datetime', inplace=True)
        get_data_frame = get_data_frame.tz_convert('UTC')
        get_data_frame.reset_index(inplace=True)
        get_data_frame.set_index('datetime', inplace=True)

        return get_data_frame















