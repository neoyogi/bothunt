import random
import netaddr
import threading
import logging
import redis

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] (%(threadName)-10s) %(message)s')

rclient = redis.Redis()

def send_data(host="localhost", port=6379, message=None):
    name = message["pos"]+":"+message["ip"]
    count_dict = {"count":1}
    print name

    if message:
        # print message["pos"]
        # rclient.zadd(message["pos"], message["ip"], "0.0")
        # rclient.zincrby(name=message["pos"], value=message["ip"], amount=1)
        # values = rclient.zrange(name=message["pos"], start=0, end=-1)
        # print values
        if rclient.hexists(name=name, key="count"):
            rclient.hincrby(name=name, key="count", amount=1)
            # print("incremented")
        else:
            rclient.hmset(name=name, mapping=count_dict)
            rclient.set(name="time2-"+name, value="")
            rclient.expire(name="time2-"+name, time=2)
            rclient.set(name="time4-"+name, value="")
            rclient.expire(name="time4-"+name, time=4)
            rclient.set(name="time6-"+name, value="")
            rclient.expire(name="time6-"+name, time=6)
            # print("added")
        result = rclient.hgetall(name=message["pos"]+":"+message["ip"])
        print result

def build_sample_data(ip="", count=0, pos=""):
    data = {
        "ip":ip,
        "count":count,
        "pos": pos
    }
    return data

def gen_hex(chars):
    return ''.join(random.choice('1234567890ABCDEF') for i in range(chars))

def gen_random(chars):
    return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for i in range(chars))

def send_sample_data(count):
    ip_range = list(netaddr.iter_iprange("11.0.0.1", "11.0.255.255"))
    pos = gen_hex(16)
    ip_address = str(random.choice(ip_range))
    for i in xrange(count):
        # url = "http://"+gen_random(7)+".com"+"/some"+"/url"+gen_random(6)+"/"+gen_random(6)
        request_data = build_sample_data(ip=ip_address, pos=pos)
        send_data("localhost", 6379, message=request_data)

if __name__ == "__main__":
    for i in range(2):
        t = threading.Thread(name="thread: " + str(i), target= send_sample_data, args=(3,))
        t.start()
