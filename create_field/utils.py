from sqlalchemy import create_engine
import pymysql
import time

import sys
import os


database = os.environ.get('database', 'scigene_database_field')

# read config.yaml
import yaml
with open('config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
field_info = config[database]

# database_mapping = {
#     '77088390': 'scigene_database_field',
#     '121332964': 'scigene_physics_field'
# }
# database = database_mapping[field_]

def create_connection(database):
    conn = pymysql.connect(host='localhost',
                                user='root',
                                password='root',
                                db=database,
                                charset='utf8')
    return conn, conn.cursor()


def init_connection(database):
    try:
        return create_connection(database)
    except:
        # Connect to the MySQL server without selecting a database
        conn = pymysql.connect(host='localhost', user='root', password='root')
        cursor = conn.cursor()
        cursor.execute(f"SHOW DATABASES LIKE '{database}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {database}")
        conn.commit()

        return create_connection(database)


conn, cursor = init_connection(database)
engine = create_engine('mysql+pymysql://root:root@192.168.0.140:3306/'+database)


def execute(sql):
    for _sql in sql.split(';'):
        _sql = _sql.strip()
        if _sql == '':
            continue
        print('*', _sql)
        t = time.time()
        cursor.execute(_sql)
        conn.commit()
        print('* time:', time.time()-t)

def try_execute(sql):
    try:
        cursor.execute(sql)
    except:
        pass
    conn.commit()