from woocommerce_api import get_data_from_woocommerce
from mssql import insert_order_to_mssql
from config import ORDERS_ENDPOINT, PRODUCTS_ENDPOINT, PROCESS_INTERVAL
from utils import get_or_create_customer, save_order_to_mssql
from products_update import update_products
import time
import datetime

def last_run_time():
    try:
        with open("last_run.log", "r") as f:
            last_run = f.readline()
            return datetime.datetime.strptime(last_run, '%Y-%m-%d %H:%M:%S')
    except:
        return None

def update_last_run_time():
    with open("last_run.log", "w") as f:
        f.write(str(datetime.datetime.now()))

def process_orders():
    orders = get_data_from_woocommerce(ORDERS_ENDPOINT)
    
    if not orders:
        return

    for order in orders:
        email = order['billing']['email']
        cari_kod = get_or_create_customer(order, email)
        save_order_to_mssql(order, cari_kod)

def main():
    while True:
        process_orders()  # Sipariş işlemleri
        
        last_run = last_run_time()
        if not last_run or (datetime.datetime.now() - last_run >= datetime.timedelta(days=1)):
            update_products()
            update_last_run_time()

        time.sleep(PROCESS_INTERVAL)  # İşlemler tamamlandıktan sonra belirli süre bekler.

if __name__ == "__main__":
    main()
