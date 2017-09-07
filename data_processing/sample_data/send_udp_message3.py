import time
import json
import random
import netaddr
import threading
import logging
from pprint import pprint
import requests
from multiprocessing import cpu_count, Queue, Process, Pool


logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] (%(threadName)-10s) %(message)s')

def send_data(host="localhost", port=80, message=None):
    start = time.time()
    result = requests.post("http://"+host+":"+str(port)+"/logs", data=message, headers={"Content-Type":"application/json"})
    end = time.time()
    print float(end-start)
    print result.status_code
    print result.text


def build_sample_data(mode=True, http_referrer="http://someip", site_url = "http://someip/url",  cookie_headers=None, ip_address="", user_id="someuser", user_agent=None, content_type=None, accept=None, connection=None, accept_encoding=None, accept_language=None, cache_control=None):
     data = {
        "http_referrer": http_referrer,
        "site_url": site_url,
        "cookie_headers": cookie_headers,
        "ip_address" : ip_address,
        "user_id" : user_id,
        "cache_control":cache_control,
        "accept_language":accept_language,
        "accept_encoding":accept_encoding,
        "accept":accept,
        "connection":connection,
        "content_type":content_type,
        "user_agent":user_agent
    }
    # json_data = json.dumps(data)
     return data

def gen_random(chars):
    return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for i in range(chars))

def gen_hex(chars):
    return ''.join(random.choice('1234567890ABCDEF') for i in range(chars))

def send_sample_data(count):
    print("Count: " + str(count))
    mode = True
    ip_range = list(netaddr.iter_iprange("11.0.0.1", "11.0.0.255"))
    user_id = "root"
    user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"
    content_type = "text/plain; charset=\"us-ascii\""
    accept = "application/xml, application/xhtml+xml, text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5"
    connection = "keep-alive"
    accept_encoding = "gzip, e, sdch"
    accept_language = "en-US,en;q=0.8"
    cache_control = "max-age=0"
    final_data = {
        "cid":"z_29827261",
        "sid":"1e40d2bd"
    }
    connection_data = list()
    for _ in range(count):
        http_referrer = "http://"+gen_random(5)+".com"+"/"+"some"+"/url"+""+ gen_random(5)
        site_url = "http://"+gen_random(7)+".com"+"/some"+"/url"+gen_random(6)+"/"+gen_random(6)
        cookie_headers = [gen_random(16), gen_random(16), gen_random(16)]
        ip_address = str(random.choice(ip_range))
        request_data = build_sample_data(mode=mode, http_referrer=http_referrer, site_url=site_url, user_agent=user_agent, content_type=content_type, accept=accept, connection=connection, accept_encoding=accept_encoding, accept_language=accept_language, cache_control=cache_control, cookie_headers=cookie_headers, ip_address=ip_address, user_id=user_id)
        connection_data.append(request_data)
    final_data["data"] = connection_data
    # pprint(final_data, indent=2, width=1)
    send_data("localhost", 80, message=json.dumps(final_data))

if __name__ == "__main__":
    for _ in range(100000):
        t = threading.Thread(name="thread: " + str(_), target= send_sample_data, args=(20,))
        t.setDaemon(True)
        t.start()
        t.join()
