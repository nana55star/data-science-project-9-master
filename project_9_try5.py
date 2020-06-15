import pandas as pd
import psycopg2

connection = psycopg2.connect(user="postgres", password="barmej", host="127.0.0.1", port="5432", database="imdb_database")

cursor = connection.cursor()

cursor.execute("SELECT label_name, label_id FROM label_types")
result1 = cursor.fetchall()
labels = {}
labels = dict(result1)
labels

data = pd.read_csv('IMDB_Dataset.csv')

data.insert(0, 'id_label', range(1,1+len(data)))

data.drop(columns=['review'], inplace=True)

data['Label_number']=[labels[x] for x in data['sentiment']]

data.drop(columns=['sentiment'], inplace=True)
data.insert(2, 'label_date', pd.datetime.now().replace(microsecond=0))

# creating column list for insertion
cols = ",".join([str(i) for i in data.columns.tolist()])

# Insert DataFrame recrds one by one.
for i,row in data.iterrows():
    sql = "INSERT INTO data_labeling2 (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql, tuple(row))

    connection.commit()
# Execute query
sql = "SELECT * FROM data_labeling WHERE id_label=4"
cursor.execute(sql)
result = cursor.fetchall()
for i in result:
    print(i)
