from woocommerce_api import get_data_from_woocommerce
from mssql import insert_order_to_mssql
from config import ORDERS_ENDPOINT, PRODUCTS_ENDPOINT

def main():
    orders = get_data_from_woocommerce(ORDERS_ENDPOINT)
    for order in orders:
        insert_order_to_mssql(order)
    
    products = get_data_from_woocommerce(PRODUCTS_ENDPOINT)
    for product in products:
        insert_order_to_mssql(product)

if __name__ == "__main__":
    main()