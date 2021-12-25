import psycopg2
import csv
import decimal
import matplotlib.pyplot as plt
import json
username = 'fedorchenko'
password = '123'
database = 'fedorchenko1_DB'
host = 'localhost'
port = '5432'
OUTPUT_FILE_T = 'fedorchenko_DB_{}.json'

TABLES = [
    'purchase',
    'conditions',
    'vegetables',
    'purch_veg',
    'purch_cond'
]


conn = psycopg2.connect(user=username, password=password, dbname=database)
data = {}
with conn:
    cur = conn.cursor()

    for table in TABLES:
        cur.execute('SELECT * FROM ' + table)
        rows = []
        fields = [x[0] for x in cur.description]

        for row in cur:
            rows.append(dict(zip(fields, row)))

        data[table] = rows

with open('data.json', 'w') as outf:
    json.dump(data, outf, default=str)