import falcon
import json
import redis
import uuid
from multiprocessing import cpu_count, Process, Queue, get_logger
from multiprocessing.queues import Empty
from elasticsearch import Elasticsearch
from elasticsearch import helpers
# from elasticsearch import ElasticsearchException, SerializationError, ConnectionTimeout, TransportError
from elasticsearch.helpers import BulkIndexError
from collections import Counter
from redis.exceptions import ResponseError, PubSubError, ConnectionError, AuthenticationError, DataError, InvalidResponse
import datetime
from retrying import retry
import time
from urllib3.connection import ConnectionError as ConnectionErr
import sys
sys.path.append("/home/yogesh/pycharm/debug-eggs/pycharm-debug.egg")
import pydevd


mapping = {
    "settings":{
     "analysis" :{
        "analyzer": {
          "url_analyzer": {
            "type" : "custom",
            "tokenizer" : "uax_url_email",
            "filter" : "lowercase"
          },
          "comma_analyzer":{
              "type":"custom",
              "tokenizer":"comma",
              "filter" : "lowercase"
          },
          "semi_colon_analyzer":{
              "type":"custom",
              "tokenizer":"semicolon",
              "filter":"lowercase"
          }
          },
        "tokenizer":{
            "comma":{
                "type":"pattern",
                "pattern":","
            },
            "semicolon":{
                "type":"pattern",
                "pattern":";"
            }
        }
      },
    "mappings":{
        "logs":{
            "properties": {
                "referrer":{"type": "string", "analyzer": "url_analyzer"},
                "url":{"type": "string", "analyzer": "url_analyzer"},
                "accept":{"type": "string", "analyzer": ["comma_analyzer", "standard"]},
                "os":{"type":"string", "analyzer":"standard" },
                "user_agent":{"type":"string", "analyzer":"standard"},
                "accept_encoding":{"type":"string", "analyzer":"comma_analyzer"},
                "cache_control":{"type":"string", "analyzer":"standard"},
                "content_type":{"type":"string", "analyzer":["semi_colon_analyzer", "standard"]},
                "source_ip":{"type":"ip"},
                "cid":{"type":"string", "analyzer":"standard"},
                "sid":{"type":"string", "analyzer":"standard"},
                "accept_language":{"type":"string", "analyzer":["comma_analyzer", "standard"]},
                "connection":{"type":"string", "analyzer":"standard"}
            } } } } }

def retry_on_redis_error(exc):
    if isinstance(exc, ConnectionError):
        print(exc.message)
        return True
    elif isinstance(exc, PubSubError):
        print(exc.message)
        return True
    elif isinstance(exc, AttributeError):
        print(exc.message)
        return True
    else:
        return False

def _generate_id():
    return str(uuid.uuid4())[:4]

class SetESValues(Process):
    def __init__(self, data_queue, failed_data_queue):
        Process.__init__(self)
        self.data_queue = data_queue
        self.failed_data_queue = failed_es_queue

    def run(self):
        while True:
            try:
                es_ip, es_port, data = self.data_queue.get(True)
                self.insertIntoES(es_ip, es_port, data)
            except Empty:
                continue

    def insertIntoES(self, es_ip, es_port, data):
        try:
            es_client = Elasticsearch(hosts=[{"host":es_ip, "port":es_port}])
            pydevd.settrace('localhost', port=9000, stdoutToServer=True, stderrToServer=True)
            helpers.bulk(client=es_client, actions=data)
        except ConnectionErr:
            self.failed_data_queue.put((es_ip, es_port, data, 1))
        except BulkIndexError:
            es_client.indices.create(index=data[0]["_index"], ignore=400, body=mapping)
            helpers.bulk(client=es_client, actions=data)

class SetFailedESValues(Process):
    def __init__(self, data_queue):
        Process.__init__(self)
        self.data_queue = data_queue

    def run(self):
        while True:
            try:
                es_ip, es_port, data, count = self.data_queue.get(True)
                if count > 10:
                    continue
                else:
                    self.insertIntoES(es_ip, es_port, data, count)
                    time.sleep(0.5)
            except Empty:
                continue
    def insertIntoES(self, es_ip, es_port, data, count):
        try:
            es_client = Elasticsearch(hosts=[{"host":es_ip, "port":es_port}])
            helpers.bulk(client=es_client, actions=data)
        except ConnectionError:
            self.data_queue.put(es_ip, es_port, data, count+1)

class SetValues(Process):
    def __init__(self, data_queue):
        Process.__init__(self)
        self.data_queue = data_queue
        self.redis_connection_pool = dict()

    def run(self):
        while True:
            try:
                # start_time = time.time()
                ip, ip_count, thresholds, cid, blocking_expire, threshold_time, dest_redis_info = self.data_queue.get(True, 0.50)
                self.setValues(ip, ip_count, thresholds, cid, blocking_expire, threshold_time, dest_redis_info)
                # print str(time.time() - start_time)
            except Empty:
                continue

    def setValues(self, ip, ip_count, thresholds, cid, blocking_expire, threshold_time, dest_redis_info):
        if dest_redis_info in self.redis_connection_pool:
            dest_redis_pool = self.redis_connection_pool[dest_redis_info]
        else:
            dest_redis_pool = redis.ConnectionPool(host=dest_redis_info[0], port=dest_redis_info[1], db=dest_redis_info[2])
            self.redis_connection_pool[dest_redis_info] = dest_redis_pool
        dest_redis_con = redis.Redis(connection_pool=dest_redis_pool)
        dest_redis_pipe = redis.Redis(connection_pool=dest_redis_pool)
        unique_id = _generate_id()
        name = "blocked_"+":"+ip+":"+cid
        if dest_redis_con.exists(name) is False:
            with dest_redis_pipe.pipeline() as redis_pipe:
                redis_pipe.multi()
                for threshold in thresholds:
                    threshold_time_value = int(threshold_time[threshold+"_time"])
                    dest_key = threshold +":"+ip
                    count_after_inc = dest_redis_con.incrby(name="x_"+":"+dest_key, amount=ip_count)
                    # redis_pipe.expire(name="x_"+":"+dest_key, time = threshold_time_value*2)
                    redis_pipe.setex(name="b_:"+dest_key+":"+str(ip_count)+":"+unique_id+":"+cid, value="", time=threshold_time_value)
                    if int(count_after_inc) >= int(thresholds[threshold]):
                        redis_pipe.setex(name=name, value="", time=int(blocking_expire))
                        break
                redis_pipe.execute()

redis_client = redis.ConnectionPool(host='localhost', port=6379, db=0)
collector_queue = Queue()
elasticsearch_queue = Queue()
failed_es_queue = Queue(maxsize=100)

redis_process_list = []
for _ in range(int(cpu_count())):
    add_to_redis_es_proc = SetValues(collector_queue)
    add_to_redis_es_proc.daemon = True
    add_to_redis_es_proc.start()
    redis_process_list.append(add_to_redis_es_proc)
print redis_process_list

es_process_list = []
for _ in range(int(cpu_count())*2):
    add_to_es_proc = SetESValues(elasticsearch_queue, failed_es_queue)
    add_to_es_proc.daemon = True
    add_to_es_proc.start()
    es_process_list.append(add_to_es_proc)

for _ in range(int(cpu_count()/2)):
    add_to_failed_es_proc = SetFailedESValues(failed_es_queue)
    add_to_failed_es_proc.daemon = True
    add_to_failed_es_proc.start()
    es_process_list.append(add_to_failed_es_proc)

print es_process_list

class Collect_logs(object):
    def __init__(self, redis_client, collector_queue, es_queue):
        self.redis_client = redis_client
        self.collector_queue = collector_queue
        self.es_queue = es_queue

    def on_post(self, req, resp):
        """Handle POST request to process the logs"""
        if req.content_type.lower() == "application/json":
            body = req.stream.read()
            if not body:
                raise falcon.HTTPBadRequest("Valid data in the body field is required", "No data in the body!")
            else:
                try:
                    request_data = json.loads(body)
                    # print json.dumps(request_data, sort_keys=True, indent=4)
                except Exception as e:
                    raise falcon.HTTPBadRequest("Valid JSON is required", e.message)
                self.parse_logs(request_data)
                return falcon.HTTP_200
        else:
            raise falcon.HTTPBadRequest("A valid JSON content-type is required", "please check the content type!")

    def parse_logs(self, http_logs):
        redis_connection = redis.Redis(connection_pool=self.redis_client)
        if redis_connection.hexists(http_logs["cid"], http_logs["sid"]):
            customer_hash = redis_connection.hgetall(name=http_logs["cid"])
            try:
                redis_ip = customer_hash["redis_ip"]
                redis_port = int(customer_hash["redis_port"])
                redis_db = int(customer_hash["redis_db"])
                self.es_ip = customer_hash["es_ip"]
                self.es_port = int(customer_hash["es_port"])
                thresholds = {
                    "threshold_1":int(customer_hash["threshold_1"]),
                    "threshold_2":int(customer_hash["threshold_2"]),
                    "threshold_3":int(customer_hash["threshold_3"]),
                    "threshold_4":int(customer_hash["threshold_4"])
                }
                thresholds_time = {
                    "threshold_1_time":int(customer_hash["threshold_1_time"]),
                    "threshold_2_time":int(customer_hash["threshold_2_time"]),
                    "threshold_3_time":int(customer_hash["threshold_3_time"]),
                    "threshold_4_time":int(customer_hash["threshold_4_time"])
                }
                self.blocking_expire = int(customer_hash["blocking_expire"])
            except KeyError as e:
                raise KeyError("Please check the following settings - redis ip, port, db and es - ip, port")
            del(customer_hash)
            if redis_ip and type(redis_ip) is str:
                self.cid = http_logs["cid"]
                self.dest_redis_pool = redis.ConnectionPool(host=redis_ip, port=int(redis_port), db=int(redis_db))
                req_ip = [_["source_ip"] for _ in http_logs["data"]]
                req_ip = Counter(req_ip)
                for ip in req_ip:
                    try:
                        self.collector_queue.put((ip,req_ip[ip], thresholds, self.cid, self.blocking_expire, thresholds_time, (redis_ip, redis_port, redis_db)))
                    except IOError as e:
                        print e.message

            datenow = datetime.datetime.now()
            index_to_insert = http_logs["cid"]+"."+str(datenow.day)+"." + str(datenow.month) + "." + str(datenow.year)

            es_insertion_data = list()
            for http_single_log in http_logs["data"]:
                log_data = dict()
                log_data["_index"] = index_to_insert
                log_data["_type"] = "logs"
                log_data["_source"] = http_single_log
                es_insertion_data.append(log_data)

            # try:
            self.es_queue.put((self.es_ip, self.es_port, es_insertion_data))
            # except IOError as e:
            #     print e.message

api = application = falcon.API()
logs_collector = Collect_logs(redis_client, collector_queue, elasticsearch_queue)
api.add_route("/logs", logs_collector)
