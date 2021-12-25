import psycopg2
import csv
import decimal
import matplotlib.pyplot as plt
username = 'fedorchenko'
password = '123'
database = 'fedorchenko1_DB'
host = 'localhost'
port = '5432'

INPUT_CSV_FILE = 'Vegetables.csv'


query_create_purchase = '''
CREATE table purchase(
    purch_id int primary key not null,
    price int,
    temperature int
);
'''

query_delete_purchase = '''
DELETE FROM purchase
'''

query_insert_purchase = '''
INSERT INTO purchase(purch_id, price, temperature) VALUES (%s, %s, %s)
'''



query_create_vegetables = '''
CREATE TABLE Vegetables (
    vegetable_id int  NOT NULL PRIMARY KEY,
    vegetable_name char(256)  NOT NULL
);
'''

query_delete_vegetables = '''
DELETE FROM vegetables
'''

query_insert_vegetables = '''
INSERT INTO vegetables(vegetable_id, vegetable_name) VALUES (%s, %s)
'''


query_create_purch_veg = '''
CREATE TABLE Purch_veg (
    purch_id int NOT NULL PRIMARY KEY,
    vegetable_id int NOT NULL,
    CONSTRAINT FK_vegetable_id FOREIGN KEY (vegetable_id) 
        REFERENCES Vegetables(vegetable_id),
    CONSTRAINT FK_purch_id FOREIGN KEY (purch_id) 
        REFERENCES Purchase(purch_ID)
);
'''

query_delete_purch_veg = '''
DELETE FROM purch_veg
'''

query_insert_purch_veg = '''
INSERT INTO purch_veg(purch_id, vegetable_id) VALUES (%s, %s)
'''

query_create_Conditions = '''
        CREATE TABLE Conditions (
        condition_id int  NOT NULL PRIMARY KEY,
        condition_name char(256)  NOT NULL
);
'''
query_insert_Conditions = '''
INSERT INTO conditions(condition_id, condition_name) VALUES (%s, %s)
'''

query_delete_conditions = '''
DELETE FROM conditions
'''

query_create_purch_cond = '''
CREATE TABLE Purch_cond (
purch_id int NOT NULL PRIMARY KEY,
condition_id int NOT NULL,
CONSTRAINT FK_condition_id FOREIGN KEY (condition_id) 
    REFERENCES Conditions(condition_id),
CONSTRAINT FK_purch_id FOREIGN KEY (purch_id) 
    REFERENCES Purchase(purch_id)
);
'''

query_delete_purch_cond = '''
DELETE FROM purch_cond
'''

query_insert_purch_cond = '''
INSERT INTO purch_cond(purch_id, condition_id) VALUES (%s, %s)
'''

conn = psycopg2.connect(user=username, password=password, dbname=database)

with conn:
    cur = conn.cursor()
    cur.execute('drop table if exists purch_veg')
    cur.execute('drop table if exists purch_cond')
    cur.execute('drop table if exists conditions')
    cur.execute('drop table if exists vegetables')
    cur.execute('drop table if exists purchase')
    cur.execute(query_create_purchase)
    cur.execute(query_delete_purchase)

    with open(INPUT_CSV_FILE, 'r') as inf:
        reader = csv.DictReader(inf)
        for idx, row in enumerate(reader):
            values = (idx, row['Price per kg'], row['Temp'])
            cur.execute(query_insert_purchase, values)
    cur = conn.cursor()

    cur.execute(query_create_vegetables)
    cur.execute(query_delete_vegetables)


    cur.execute(query_create_purch_veg)
    cur.execute(query_delete_purch_veg)

    with open(INPUT_CSV_FILE, 'r') as inf:
        reader = csv.DictReader(inf)
        i = 0
        unique_vegetables = {}
        for idx, row in enumerate(reader):
            if row['Vegetable'] not in unique_vegetables:
                unique_vegetables[row['Vegetable']] = i
                values = (i, row['Vegetable'])
                i += 1
                cur.execute(query_insert_vegetables, values)
            cur.execute(query_insert_purch_veg, (idx, unique_vegetables[row['Vegetable']]))


    cur.execute(query_create_Conditions)
    cur.execute(query_delete_conditions)


    cur.execute(query_create_purch_cond)
    cur.execute(query_delete_purch_cond)

    with open(INPUT_CSV_FILE, 'r') as inf:
        reader = csv.DictReader(inf)
        i = 0
        unique_conditions = {}
        for idx, row in enumerate(reader):
            if row['Vegetable condition'] not in unique_conditions:
                unique_conditions[row['Vegetable condition']] = i
                values = (i, row['Vegetable condition'])
                i += 1
                cur.execute(query_insert_Conditions, values)
            cur.execute(query_insert_purch_cond, (idx, unique_conditions[row['Vegetable condition']]))
    conn.commit()
