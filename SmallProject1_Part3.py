import pandas as pd
import psycopg2

conn = psycopg2.connect(user="postgres",
                        password="ubuntu",
                        host="192.168.0.103",
                        port="5432",
                        database="sp1_invoice")

df_awaltahun = pd.read_sql('select*from awaltahun',conn)
df_akhirtahun = pd.read_sql('select*from akhirtahun',conn)
df_belumbayar = pd.read_sql('select*from belumbayar',conn)

invoice_id = input('Cek status bayar Anda dengan menginputkan nomor invoice Anda: ')

if invoice_id in df_awaltahun['invoiceid'].values:
    print('Terima kasih karena Anda telah melakukan pembayaran pada invoice',invoice_id,'. Anda berhak mendapatkan diskon awal tahun sebesar 10%!')
elif invoice_id in df_akhirtahun['invoiceid'].values:
    print('Terima kasih karena Anda telah melakukan pembayaran pada invoice',invoice_id,'. Anda berhak mendapatkan diskon akhir tahun sebesar 20%!')
elif invoice_id in df_belumbayar['invoiceid'].values:
    print('Yuk segera bayar tagihan invoice', invoice_id,', dan dapatkan diskon sebesar 15%!')
else:
    print('Anda sedang mabuk. Masukkan nomor invoice yang benar.')