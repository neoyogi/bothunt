import elasticsearch
import redis
import logging, threading
from multiprocessing import Queue
import requests
import time

shared_queue = Queue()

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

queue_condition = threading.Condition()
def update_redis(condition):
    with condition:
        while shared_queue.empty():
            logging.info("[%s] - waiting for elements in the queue.." %threading.current_thread().name)
            condition.wait()
        else:
            logging.debug("Waiting to get the elements")
            value = shared_queue.get()
            logging.debug(value.__class__)
            # shared_queue.task_done()

def queue_task(condition):
    logging.debug("starting to queue the tasks")
    with condition:
        # while True:
        #     time.sleep(2)
        es_license = requests.get("http://localhost:9200/collector/license/_search").text
            # logging.debug(es_license)
        shared_queue.put(es_license)
        condition.notifyAll()

update_redis_thread = threading.Thread(target=update_redis, args=(queue_condition,))
update_redis_thread.start()

prod = threading.Thread(name='queue_task_thread', target=queue_task, args=(queue_condition,))
prod.start()
