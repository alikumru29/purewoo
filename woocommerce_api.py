import http.client
from base64 import b64encode
from config import WOOCOMMERCE_KEY, WOOCOMMERCE_SECRET, WC_URL
import json

def get_data_from_woocommerce(endpoint):
    authString = b64encode(bytes(WOOCOMMERCE_KEY + ':' + WOOCOMMERCE_SECRET, 'utf-8')).decode('ascii')
    headers = {
        'Authorization': 'Basic %s' % authString,
    }
    
    conn = http.client.HTTPSConnection(WC_URL)
    conn.request("GET", endpoint, headers=headers)
    res = conn.getresponse()
    data = res.read().decode('utf-8')  
    result = json.loads(data)
    
    return result

def get_products_from_woocommerce(endpoint, page_number=1):
    authString = b64encode(bytes(WOOCOMMERCE_KEY + ':' + WOOCOMMERCE_SECRET, 'utf-8')).decode('ascii')
    headers = {
        'Authorization': 'Basic %s' % authString,
    }

    paginated_endpoint = endpoint + f"?page={page_number}"
    conn = http.client.HTTPSConnection(WC_URL)
    conn.request("GET", paginated_endpoint, headers=headers)
    res = conn.getresponse()
    data = res.read().decode('utf-8')  
    result = json.loads(data)

    return result, res.status  # Veriyi ve HTTP yanıt kodunu döndürün

def post_data_to_woocommerce(endpoint, data):
    authString = b64encode(bytes(WOOCOMMERCE_KEY + ':' + WOOCOMMERCE_SECRET, 'utf-8')).decode('ascii')
    headers = {
        'Authorization': 'Basic %s' % authString,
        'Content-Type': 'application/json'
    }
    
    conn = http.client.HTTPSConnection(WC_URL)
    conn.request("POST", endpoint, body=json.dumps(data), headers=headers)
    res = conn.getresponse()
    response_data = res.read().decode('utf-8')

    result = json.loads(response_data)
    return result

