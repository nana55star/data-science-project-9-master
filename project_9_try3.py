import pandas as pd
data = pd.read_csv('IMDB_Dataset.csv')
data.drop(columns=['sentiment'], inplace=True)
data.insert(0, 'id', range(1,1+len(data)))
data['input_data']=data['review']
data.drop(columns=['review'], inplace=True)
data.insert(2, 'input_date', pd.datetime.now().replace(microsecond=0))
import psycopg2
connection = psycopg2.connect(user="postgres", password="barmej", host="127.0.0.1", port="5432", database="imdb_database")
cursor = connection.cursor()
# creating column list for insertion
cols = ",".join([str(i) for i in data.columns.tolist()])

# Insert DataFrame recrds one by one.
for i,row in data.iterrows():
    sql = "INSERT INTO data_input (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql, tuple(row))

    # the connection is not autocommitted by default, so we must commit to save our changes
    connection.commit()
# Execute query
sql = "SELECT * FROM data_input"
cursor.execute(sql)

# Fetch all the records
result = cursor.fetchall()
for i in result:
    print(i)
