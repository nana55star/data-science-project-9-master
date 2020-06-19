import psycopg2
connection = psycopg2.connect(user="postgres", password="barmej", host="127.0.0.1", port="5432", database="imdb_database")

cursor = connection.cursor()

# Execute query
sql = "SELECT * FROM data_input2 WHERE id<2"
cursor.execute(sql)
# Fetch all the records
result = cursor.fetchall()
for i in result:
    print(i)

sq2 = "SELECT * FROM data_labeling3 WHERE id_label < 6"
cursor.execute(sq2)
result = cursor.fetchall()
for i in result:
    print(i)

