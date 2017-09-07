from multiprocessing import Process, Queue, cpu_count
import redis
from multiprocessing.queues import Empty
from time import sleep
from redis.exceptions import ResponseError, PubSubError, ConnectionError, AuthenticationError, DataError, InvalidResponse
from retrying import retry
import time

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

class Listener(Process):
    def __init__(self, r, channels, redis_events_queue):
        Process.__init__(self)
        self.redis_ip, self.redis_port, self.redis_db = r
        self.channels = channels
        self.redis_events_queue = redis_events_queue

    @retry(stop_max_attempt_number=2000, wait_exponential_multiplier=100, wait_exponential_max=1000, retry_on_exception=retry_on_redis_error)
    def run(self):
        self.redis = redis.Redis(host=self.redis_ip, port=self.redis_port, db=self.redis_db)
        self.pubsub = self.redis.pubsub()
        self.pubsub.psubscribe(self.channels)
        for item in self.pubsub.listen():
            if item["data"] != 1:
               # print item['channel'], ":", item["data"]
                self.redis_events_queue.put(((self.redis_ip, self.redis_port, self.redis_db),item["data"]))

class Process_events_in_queue(Process):
    def __init__(self, events_queue):
        Process.__init__(self)
        self.events_queue = events_queue
        self.list_of_redis_pools = dict()

    def run(self):
        while True:
            try:
                redis_info, expired_event= self.events_queue.get(True, 0.2)
                self.perform_reset(redis_info, expired_event)
            except Empty:
                continue

    @retry(stop_max_attempt_number=100, retry_on_exception=retry_on_redis_error)
    def perform_reset(self, redis_info, expired_event):
        if redis_info not in self.list_of_redis_pools:
            self.list_of_redis_pools[redis_info] = redis.ConnectionPool(host=redis_info[0], port=redis_info[1], db=redis_info[2])
        redis_client = redis.Redis(connection_pool=self.list_of_redis_pools[redis_info])
        expired_event_split = expired_event.split(":")
        if expired_event_split[0] == "b_":
            key_to_reset = expired_event_split[1]+":"+expired_event_split[2]
            reset_value = int(expired_event_split[-3])
            value_after_decr = redis_client.decr(name="x_"+":"+key_to_reset, amount=int(reset_value))
            if int(value_after_decr) <= 0:
                redis_client.delete("x_"+":"+key_to_reset,)
        elif expired_event_split[0] == "blocked_":
            keys_to_delete = redis_client.keys("x_*:"+expired_event_split[1])
            if keys_to_delete and len(keys_to_delete) > 0:
                redis_client.delete(*keys_to_delete)

@retry(stop_max_attempt_number=10, wait_fixed=500, retry_on_exception=retry_on_redis_error)
def spawn_listener_proc(redis_ip=None, redis_port=None, redis_db=None, cid=None):
    redis_conn = redis.Redis(host=redis_ip, port=redis_port, db=redis_db)
    if redis_conn.config_set("notify-keyspace-events", "xKE"):
        client = Listener((redis_ip, redis_port, redis_db), ["__keyevent@"+str(redis_db)+"__:expired"], redis_events_queue)
        client.name = cid
        client.daemon = True
        client.start()
        return client

@retry(stop_max_attempt_number=20, wait_exponential_multiplier=100, wait_exponential_max=1000, retry_on_exception=retry_on_redis_error)
def get_redis_collector_info_and_spawn_process(redis_pool):
    dict_of_processes = dict()
    redis.Redis(connection_pool=redis_pool).config_set("notify-keyspace-events", "AKE")
    redis_local_conn = redis.Redis(connection_pool=redis_pool)
    list_of_keys = redis_local_conn.keys("z_*")
    for key in list_of_keys:
        try:
            record_hash = redis_local_conn.hgetall(key)
            record_redis_ip = record_hash["redis_ip"]
            record_redis_port = int(record_hash["redis_port"])
            record_redis_db = int(record_hash["redis_db"])
            dict_of_processes[key] = spawn_listener_proc(redis_ip=record_redis_ip, redis_port=record_redis_port, redis_db=record_redis_db, cid=key)
        except KeyError as e:
            print e.message
            pass
        except ResponseError as e:
            print e.message
            pass
        except ConnectionError as e:
            print ("failed to connect to the redis with key:" + str(key))
    return dict_of_processes

def process_new_collector_hash_on_redis(redis_pool, dict_of_processes, list_of_processes_events_proc):
    redis_local_conn = redis.Redis(connection_pool=redis_pool)
    redis_conn_pubsub = redis.Redis(connection_pool=redis_pool).pubsub()
    redis_conn_pubsub.psubscribe(["__keyevent@0__:hset", "__keyevent@0__:del"])
    for item in redis_conn_pubsub.listen():
        print item
        if item['data'] == "KILL":
            redis_conn_pubsub.unsubscribe()
            dict_of_processes.update()
            for proc in dict_of_processes:
                if dict_of_processes[proc].is_alive():
                    dict_of_processes[proc].terminate()
                dict_of_processes.pop(proc, None)

            for proc in list_of_process_events_proc:
                if proc.is_alive():
                    proc.terminate()
                list_of_process_events_proc.remove(proc)
            break

        elif item["pattern"] is None:
            continue
        elif item["pattern"] == "__keyevent@0__:hset":
            try:
                record_hash = redis_local_conn.hgetall(item["data"])
                record_redis_ip = record_hash["redis_ip"]
                record_redis_port = int(record_hash["redis_port"])
                record_redis_db = int(record_hash["redis_db"])
                print("spawing listener process" + item["data"])
                if not dict_of_processes.has_key(item["data"]):
                    dict_of_processes[item["data"]] = spawn_listener_proc(redis_ip=record_redis_ip, redis_port=record_redis_port, redis_db=record_redis_db, cid=item["data"])
            except ResponseError:
                pass
            except KeyError:
                pass

        elif item["pattern"] == "__keyevent@0__:del":
            print("removing process")
            if dict_of_processes.has_key(item["data"]):
                if dict_of_processes[item["data"]].is_alive():
                    dict_of_processes[item["data"]].terminate()
                dict_of_processes.pop(item["data"], None)

if __name__ == "__main__":
    redis_events_queue = Queue()
    redis_pool = redis.ConnectionPool(host="127.0.0.1", port="6379", db=0)
    list_of_process_events_proc = list()

    for _ in range(int(cpu_count())*2):
        process_events = Process_events_in_queue(redis_events_queue)
        process_events.daemon = True
        process_events.start()
        list_of_process_events_proc.append(process_events)

    dict_of_processes = dict()
    proc_dict = get_redis_collector_info_and_spawn_process(redis_pool)
    dict_of_processes = proc_dict.copy()
    dict_of_processes.update()

    process_new_collector_hash_on_redis(redis_pool=redis_pool, dict_of_processes=dict_of_processes, list_of_processes_events_proc=list_of_process_events_proc )
