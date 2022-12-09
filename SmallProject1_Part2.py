import pandas as pd
import psycopg2
import psycopg2.extras as extras

conn = psycopg2.connect(user="postgres",
                        password="ubuntu",
                        host="192.168.0.103",
                        port="5432",
                        database="sp1_invoice")

df = pd.read_sql('select*from masterdata',conn)

df_sudahbayar = df.query("paymentstatus in ('Successfully captured using credit_card','Transaction successfully transfered using credit_card')")
df_date = pd.DataFrame(df_sudahbayar['date'].str.split(' ',1,expand=True).rename(columns={0:'date_only',1:'time_only'}))
df_bulan = pd.DataFrame(df_date['date_only'].str.split('/',2,expand=True)).rename(columns={0:'bulan',1:'tanggal',2:'tahun'})

bulan = df_bulan['bulan']
bulan_integer = bulan.astype(int)

# pd.set_option('display.max_columns',None)
# # pd.set_option('display.max_rows',None)
# # print(df_sudahbayar['paymentstatus'])
# # print(df_bulan)

def execute_valuesawaltahun(conn, df_awaltahun, table):
    tuples = [tuple(x) for x in df_awaltahun.to_numpy()]
    cols = ','.join(list(df_awaltahun.columns))

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

    print("The Dataframe Awal Tahun is Inserted!")

    cursor.close()

def execute_valuesakhirtahun(conn, df_akhirtahun, table):
    tuples = [tuple(x) for x in df_akhirtahun.to_numpy()]
    cols = ','.join(list(df_akhirtahun.columns))

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

    print("The Dataframe Akhir Tahun is Inserted!")

    cursor.close()

def execute_valuesbelumbayar(conn, df_belumbayar, table):
    tuples = [tuple(x) for x in df_belumbayar.to_numpy()]
    cols = ','.join(list(df_belumbayar.columns))

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

    print("The Dataframe Belum Bayar is Inserted!")

    cursor.close()

if __name__ == '__main__':
    df_waktubayar = pd.concat([df_sudahbayar, bulan_integer], axis=1)
    df_awaltahun = df_waktubayar.query("`bulan` < 4")
    df_awaltahunfinal= df_awaltahun.drop(columns=['bulan'],axis=1)
    df_akhirtahun = df_waktubayar.query("`bulan` > 9 and `bulan` < 13")
    df_akhirtahunfinal= df_akhirtahun.drop(columns=['bulan'],axis=1)
    df_belumbayar = df.query("paymentstatus in ('Waiting customer to finish transaction using bank_transfer', 'Waiting customer to finish transaction using echannel','Payment using echannel for transaction is expired.')")

    execute_valuesawaltahun(conn, df_awaltahunfinal, 'awaltahun')
    execute_valuesakhirtahun(conn, df_akhirtahunfinal, 'akhirtahun')
    execute_valuesbelumbayar(conn, df_belumbayar, 'belumbayar')