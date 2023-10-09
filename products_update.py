import pyodbc
from config import MSSQL_SERVER, MSSQL_DATABASE, MSSQL_USERNAME, MSSQL_PASSWORD
from woocommerce_api import post_data_to_woocommerce, get_products_from_woocommerce
from config import PRODUCTS_ENDPOINT
import logging

logging.basicConfig(filename='log.txt', level=logging.ERROR, format='%(asctime)s %(levelname)s: %(message)s')


def get_local_product(sku):
    connection = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={MSSQL_SERVER};DATABASE={MSSQL_DATABASE};UID={MSSQL_USERNAME};PWD={MSSQL_PASSWORD}')
    cursor = connection.cursor()

    # SKU ile ürün bilgisini getir
    cursor.execute(f"SELECT * FROM STOKLAR WHERE sto_kod = '{sku}'")
    product_data = cursor.fetchone()

    # Fiyat bilgilerini al
    cursor.execute(f"SELECT sfiyat_fiyati FROM STOK_SATIS_FIYAT_LISTELERI WHERE sfiyat_stokkod = '{sku}' AND sfiyat_listesirano = 96")
    regular_price = str(cursor.fetchone()[0])

    cursor.execute(f"SELECT sfiyat_fiyati FROM STOK_SATIS_FIYAT_LISTELERI WHERE sfiyat_stokkod = '{sku}' AND sfiyat_listesirano = 2")
    sale_price = str(cursor.fetchone()[0])

    stock_quantity = cursor.execute(f"SELECT (dbo.fn_EldekiMiktar('{sku}') - ISNULL(dbo.fn_Stok_Siparis_Miktar('{sku}', 0, 1, GETDATE()), 0))").fetchone()[0]

    backorders, backorders_allowed, backordered = ("no", False, False)
    if product_data.sto_model_kodu == 'URETIM':
        backorders, backorders_allowed, backordered = ("notify", True, True)

    # Ebat ve ağırlık bilgilerini düzenleyin
    weight = str(product_data.sto_birim1_agirlik).replace(',', '.')
    length = str(product_data.sto_birim1_boy / 10).replace(',', '.')
    width = str(product_data.sto_birim1_en / 10).replace(',', '.')
    height = str(product_data.sto_birim1_yukseklik / 10).replace(',', '.')

    product = {
        'sku': sku,
        'name': product_data.sto_kisa_ismi,
        'price': sale_price,
        'regular_price': regular_price,
        'sale_price': sale_price,
        'stock_quantity': stock_quantity,
        'backorders': backorders,
        'backorders_allowed': backorders_allowed,
        'backordered': backordered,
        'weight': weight,
        'dimensions': {
            'length': length,
            'width': width,
            'height': height
        }
    }

    connection.close()
    return product



def determine_fields_to_update(woocommerce_product, local_product):
    fields_to_update = {}
    changed_fields = []  # Değişen alanların bilgisini tutmak için bir liste

    # Ürün adı için kontrol
    if woocommerce_product['name'] != local_product['name']:
        fields_to_update['name'] = local_product['name']
        changed_fields.append(('name', woocommerce_product['name'], local_product['name']))

    # Fiyat bilgileri için kontrol
    if float(woocommerce_product['price']) != float(local_product['price']):
        fields_to_update['price'] = str(local_product['price'])
        changed_fields.append(('price', woocommerce_product['price'], str(local_product['price'])))

    if float (woocommerce_product['regular_price']) != float(local_product['regular_price']):
        fields_to_update['regular_price'] = local_product['regular_price']
        changed_fields.append(('regular_price', woocommerce_product['regular_price'], local_product['regular_price']))
    
    if float (woocommerce_product['sale_price']) != float(local_product['sale_price']):
        fields_to_update['sale_price'] = local_product['sale_price']
        changed_fields.append(('sale_price', woocommerce_product['sale_price'], local_product['sale_price']))

    # Stok miktarı için kontrol
    if woocommerce_product['stock_quantity'] != local_product['stock_quantity']:
        fields_to_update['stock_quantity'] = local_product['stock_quantity']
        changed_fields.append(('stock_quantity', woocommerce_product['stock_quantity'], local_product['stock_quantity']))

    # Backorder bilgileri için kontrol
    if woocommerce_product['backorders'] != local_product['backorders']:
        fields_to_update['backorders'] = local_product['backorders']
        changed_fields.append(('backorders', woocommerce_product['backorders'], local_product['backorders']))
        
    if woocommerce_product['backorders_allowed'] != local_product['backorders_allowed']:
        fields_to_update['backorders_allowed'] = local_product['backorders_allowed']
        changed_fields.append(('backorders_allowed', woocommerce_product['backorders_allowed'], local_product['backorders_allowed']))
    
    if woocommerce_product['backordered'] != local_product['backordered']:
        fields_to_update['backordered'] = local_product['backordered']
        changed_fields.append(('backordered', woocommerce_product['backordered'], local_product['backordered']))

    # Ağırlık bilgisi için kontrol
    if woocommerce_product['weight'] != local_product['weight']:
        fields_to_update['weight'] = local_product['weight']
        changed_fields.append(('weight', woocommerce_product['weight'], local_product['weight']))

    # Boyut bilgileri için kontrol
    if 'dimensions' in woocommerce_product:
        if woocommerce_product['dimensions']['length'] != local_product['dimensions']['length']:
            if 'dimensions' not in fields_to_update:
                fields_to_update['dimensions'] = {}
            fields_to_update['dimensions']['length'] = local_product['dimensions']['length']
            changed_fields.append(('length', woocommerce_product['dimensions']['length'], local_product['dimensions']['length']))
        
        if woocommerce_product['dimensions']['width'] != local_product['dimensions']['width']:
            if 'dimensions' not in fields_to_update:
                fields_to_update['dimensions'] = {}
            fields_to_update['dimensions']['width'] = local_product['dimensions']['width']
            changed_fields.append(('width', woocommerce_product['dimensions']['width'], local_product['dimensions']['width']))
        
        if woocommerce_product['dimensions']['height'] != local_product['dimensions']['height']:
            if 'dimensions' not in fields_to_update:
                fields_to_update['dimensions'] = {}
            fields_to_update['dimensions']['height'] = local_product['dimensions']['height']
            changed_fields.append(('height', woocommerce_product['dimensions']['height'], local_product['dimensions']['height']))
    else:
        fields_to_update['dimensions'] = local_product['dimensions']

    return fields_to_update, changed_fields


def update_product(product_id, sku, fields_to_update, changed_fields):
    # Sadece farklı olan alanları güncelleyin
    endpoint = f"{PRODUCTS_ENDPOINT}/{product_id}"
    post_data_to_woocommerce(endpoint, fields_to_update)
    
    # Değişiklikleri ekrana yazdır
    print(f"{sku} kodlu üründe değişiklik yapıldı.")
    for field, old_value, new_value in changed_fields:
        print(f"{field} : Eski : {old_value} - Yeni : {new_value}")

def update_products():
    all_data = []
    page_number = 1
    
    while True:
        result, status = get_products_from_woocommerce(PRODUCTS_ENDPOINT, page_number)
        
        if not result or status != 200:
            break
        
        print(f"Sayfa {page_number} alınıyor...")
        all_data.extend(result)
        page_number += 1

    for product in all_data:
        try:
            local_product = get_local_product(product['sku'])
            fields_to_update, changed_fields = determine_fields_to_update(product, local_product)
            
            if fields_to_update:
                update_product(product['id'], product['sku'], fields_to_update, changed_fields)
        except Exception as e:
            logging.error(f"SKU {product['sku']} için hata: {e}")
