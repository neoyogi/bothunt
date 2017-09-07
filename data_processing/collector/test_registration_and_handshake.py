import requests
import json

new_customer_json = {
    "name":"Test_customer",
    "subscription": 90,
    "redis_ip":"127.0.0.1",
    "redis_port":7777,
    "redis_db":2,
    "es_ip":"127.0.0.1",
    "es_port":9200,
    "threshold_1":100,
    "threshold_1_time":10,
    "threshold_2":150,
    "threshold_2_time":20,
    "threshold_3":200,
    "threshold_3_time":30,
    "threshold_4":250,
    "threshold_4_time":40,
    "blocking_expire": 120,
}

if __name__ == "__main__":
    new_registration_url = "/registration/new"
    registration_details = requests.post(url="http://localhost:80"+new_registration_url, data=json.dumps(new_customer_json), headers={"Content-Type":"application/json"})
    print registration_details.text
    print registration_details.status_code
    if registration_details.status_code == 200:
        print registration_details.text
        new_registration_details = json.loads(registration_details.text)
        handshake_json = {"cid":new_registration_details.keys()[0], "server":"apache2", "version":2.2}
        handshake_response = requests.post(url="http://localhost:80/handshake", data=json.dumps(handshake_json), headers={"Content-Type":"application/json"})
        print handshake_response.text
        print handshake_response.status_code
