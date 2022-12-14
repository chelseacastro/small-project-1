import psycopg2
import psycopg2.extras as extras
import pandas as pd

def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))

    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()

    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

    print("Dataframe is Inserted!")

    cursor.close()


conn = psycopg2.connect(user="postgres",
                        password="ubuntu",
                        host="192.168.0.103",
                        port="5432",
                        database="sp1_invoice")

df = pd.read_csv('invoice.csv')

execute_values(conn, df, 'masterdata')