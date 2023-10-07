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
