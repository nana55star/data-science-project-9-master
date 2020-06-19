#table number 1 and 2!!!!!!

import pandas as pd
import psycopg2
import datetime
connection = psycopg2.connect(user="postgres", password="barmej", host="127.0.0.1", port="5432", database="imdb_database")

cursor = connection.cursor()

cursor.execute("SELECT label_name, label_id FROM label_types")
result1 = cursor.fetchall()
labels = {}
labels = dict(result1)
labels

data = pd.read_csv('IMDB_Dataset.csv')
data.insert(0, 'id', range(1,1+len(data)))
data.insert(3, 'date', datetime.datetime.now().replace(microsecond=0))


# creating column list for insertion
scaled = pd.DataFrame()
scaled = data[['id', 'review','date']]
scaled = scaled.rename(columns={ "id": "id", "review": "input_data", "date": "input_date" })
cols1 = ",".join([str(i) for i in scaled.columns.tolist()])

data['sentiment']=[labels[x] for x in data['sentiment']]
scaled2 = pd.DataFrame()
scaled2 = data[['id', 'sentiment','date']]
scaled2 = scaled2.rename(columns={ "id": "id_label", "sentiment": "Label_number", "date": "Label_date" })
cols2 = ",".join([str(i) for i in scaled2.columns.tolist()])

for i,row in scaled.iterrows():
    sql1 = "INSERT INTO data_input2 (" +cols1 + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql1, tuple(row))
    # the connection is not autocommitted by default, so we must commit to save our changes
    connection.commit()

for i,row in scaled2.iterrows():
    sql2 = "INSERT INTO data_labeling3 (" +cols2 + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql2, tuple(row))
    # the connection is not autocommitted by default, so we must commit to save our changes
    connection.commit()
    
# Execute query
sql = "SELECT * FROM data_input2 WHERE id=4"
cursor.execute(sql)
# Fetch all the records
result = cursor.fetchall()
for i in result:
    print(i)

sq2 = "SELECT * FROM data_labeling3 WHERE id_label=4"
cursor.execute(sq2)
result = cursor.fetchall()
for i in result:
    print(i)
