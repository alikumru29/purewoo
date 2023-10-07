from decouple import config

WOOCOMMERCE_KEY = config("WOOCOMMERCE_KEY")
WOOCOMMERCE_SECRET = config("WOOCOMMERCE_SECRET")
WC_URL = "pureconcept.com.tr"

ORDERS_ENDPOINT = "/wp-json/wc/v3/orders?status=processing"
PRODUCTS_ENDPOINT = "/wp-json/wc/v3/products"

MSSQL_SERVER = config("MSSQL_SERVER")
MSSQL_DATABASE = config("MSSQL_DATABASE")
MSSQL_USERNAME = config("MSSQL_USERNAME")
MSSQL_PASSWORD = config("MSSQL_PASSWORD")
