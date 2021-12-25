import psycopg2
import matplotlib.pyplot as plt
username = 'fedorchenko'
password = '123'
database = 'Lab_2'
host = 'localhost'
port = '5432'

query_1 = '''
create view Vegitables_category as
    select count(vegitables.vegitable_name), TRIM(vegitables.vegitable_name)
        from purch_veg left join vegitables on vegitables.vegitable_id = purch_veg.vegitable_name
        group by vegitables.vegitable_name
'''

query_2 = '''
create view Vegitables_quality as
    select count(conditions.condition_name), TRIM(conditions.condition_name)
        from purch_cond left join conditions on conditions.condition_id = purch_cond.condition_name
        group by conditions.condition_name 
'''

query_3 ='''
create view Vegitables_price as
    select  avg(purchases.price), TRIM(con.condition_name) as condition_type 
        from purchases left join (
            select purch_id, conditions.condition_name from purch_cond left join conditions 
                on conditions.condition_id = purch_cond.condition_name
                ) as con
            on purchases.purchase_id = con.purch_id group by condition_type
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with conn:
    print("Database opened successfully")

    cur1 = conn.cursor()
    cur1.execute('DROP VIEW IF EXISTS Vegitables_category')
    cur1.execute(query_1)
    cur1.execute('SELECT * FROM Vegitables_category')
    vegetables = []
    total = []

    for row in cur1:
        vegetables.append(row[1])
        total.append(row[0])

    x_range = range(len(vegetables))

    figure, (bar_ax, pie_ax, graph_ax) = plt.subplots(1, 3)
    bar = bar_ax.bar(x_range, total, label='Total')
    bar_ax.set_title('Покупки')
    bar_ax.set_xlabel('Овочі')
    bar_ax.set_ylabel('Кількість покупок')
    bar_ax.set_xticks(x_range)
    bar_ax.set_xticklabels(vegetables)

    cur2 = conn.cursor()
    cur2.execute('DROP VIEW IF EXISTS Vegitables_quality')
    cur2.execute(query_2)
    cur2.execute('SELECT * FROM Vegitables_quality')
    condition = []
    total = []
    for row in cur2:
        condition.append(row[1])
        total.append(row[0])

    pie_ax.pie(total, labels=condition, autopct='%1.1f%%')
    pie_ax.set_title('Частка станів кожного товару')

    cur3 = conn.cursor()
    cur3.execute('DROP VIEW IF EXISTS Vegitables_price')
    cur3.execute(query_3)
    cur3.execute('SELECT * FROM Vegitables_price')
    condition = []
    item_price = []

    for row in cur3:
        condition.append(row[0])
        item_price.append(row[1])

    graph_ax.plot(condition, item_price, marker='o')

    graph_ax.set_xlabel('Якість товару')
    graph_ax.set_ylabel('Ціна, $')
    graph_ax.set_title('Залежність ціни від якості овочей')

    for qnt, price in zip(condition, item_price):
        graph_ax.annotate(price, xy=(qnt, price), xytext=(7, 2), textcoords='offset points')

mng = plt.get_current_fig_manager()
mng.resize(1400, 600)

plt.show()