"""
Test to connect an create a db via psycopg2.
Code inspired by:
https://www.postgresqltutorial.com/postgresql-python/connect/
https://github.com/NaysanSaran/pandas2postgresql/blob/master/src/single_insert.py
https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/
"""

import psycopg2 as ps
from psycopg2 import extras
import sys

__version__ = "1.0.0"
__author__ = "Mauricio Salazar"

class TimescaledbConnection:
    def __init__(self, username, password, host='localhost', port=5432, clear_table=False):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.clear_table = clear_table
        self.db_name = None
        self.table_name = None

        assert self.check_connection(), "Check connection to the database. Is timescaledb/docker running?"

        self.create_db()
        self.create_table()

    def check_connection(self, db_name="postgres"):
        conn = None
        try:
            print("Testing connection with PostgreSQL database...")
            conn = ps.connect(host=self.host, port=self.port, user=self.username, password=self.password, dbname=db_name)
            cur = conn.cursor()
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')
            db_version = cur.fetchone()
            print(db_version)
            cur.close()
        except (Exception, ps.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
                return True
            return False

    def create_db(self, db_name="test_python_db"):
        self.db_name = db_name
        conn = None
        try:
            conn = ps.connect(host=self.host, port=self.port, user=self.username, password=self.password)
            cur = conn.cursor()
            conn.autocommit = True
            cur.execute(f"""SELECT datname FROM pg_catalog.pg_database WHERE datname = '{self.db_name}'""")
            exists = cur.fetchone()
            if not exists:
                # cur.execute(f"""DROP DATABASE IF EXISTS {self.db_name}""")
                cur.execute(f"""CREATE DATABASE {self.db_name}""")
                print("Database created")
            else:
                print("Database exists")
            cur.close()
        except Exception as error:
            print(f"{type(error).__name__}: {error}")
            print(f"Query: {cur.query}")
            cur.close()
        else:
            conn.autocommit = False
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
        cur.close()

    def create_table(self, table_name="operation_log"):
        assert self.db_name is not None, "Create a database first"
        self.table_name = table_name
        conn = None
        try:
            conn = ps.connect(host=self.host, port=self.port, user=self.username, password=self.password, dbname=self.db_name)
            cur = conn.cursor()

            if self.clear_table:
                cur.execute(f"""DROP TABLE IF EXISTS {table_name};""")
                print("Table cleared.")

            cur.execute(f"""
                         CREATE TABLE IF NOT EXISTS {table_name} (
                            time_control TIMESTAMPTZ,
                            channel VARCHAR (100),
                            values_channel FLOAT);
                        """)
            conn.commit()
            cur.close()
        except (Exception, ps.DatabaseError) as error:
            print(error)
            cur.close()
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
        cur.close()

    def insert_data(self, df_output):
        assert self.db_name is not None, "Create a database first"
        assert self.table_name is not None, "Create a table first"

        conn = None
        try:
            conn = ps.connect(host=self.host, port=self.port, user=self.username, password=self.password, dbname=self.db_name)
            cur = conn.cursor()
            # data = [('2020-11-11 14:15:00+00:00','forecast',6.952342)]
            data = list(df_output.itertuples(index=False, name=None))
            insert_query = f"""insert into {self.table_name} (time_control, channel, values_channel) values %s"""
            extras.execute_values(cur, insert_query, data, template=None, page_size=100)
            conn.commit()
        except (Exception, ps.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

    def _connect(self, user, password, db_name):
        """ Connect to the PostgreSQL database server: Just for testing purposes """
        conn = None
        try:
            print('Connecting to the PostgreSQL database...')
            conn = ps.connect(host=self.host, port=self.port, user=user, password=password, dbname=db_name)
        except (Exception, ps.DatabaseError) as error:
            print(error)
            sys.exit(1)
        finally:
            if conn is not None:
                conn.close()
        print("Connection successful...")


if __name__ == "__main__":
    import pandas as pd

    df = pd.read_csv("..\simulation_data.csv", parse_dates=['datetimeFC']).set_index(['datetimeFC'], drop=True)
    # df = df.iloc[[0], :]  # Test with one entry
    df_new = df.reset_index()
    df_output = pd.melt(df_new, id_vars="datetimeFC", value_vars=df_new.drop('datetimeFC', axis=1), var_name="channel", value_name="values_channel")
    df_output['datetimeFC'] = df_output['datetimeFC'].astype(str)
    # df_output = df_output.round(2)

    username_db = "postgres"
    password_db = "postgres"

    db = TimescaledbConnection(username=username_db, password=password_db)
    db.insert_data(df_output)
