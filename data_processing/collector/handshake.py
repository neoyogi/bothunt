import falcon
import json
import uuid
import redis
import datetime
from elasticsearch import Elasticsearch

def _generate_id():
    return str(uuid.uuid4())[:8]

class Registration(object):
    def __init__(self, redis_client, es_client):
        self.redis_client = redis_client
        self.es_client = es_client

    def on_post(self, req, resp):
        if req.content_type.lower() == "application/json":
            body = req.stream.read()
            if not body:
                raise falcon.HTTPBadRequest("Valid data in the body field is required", "No data in the body!")
            else:
                try:
                    request_data = json.loads(body)
                except Exception as e:
                    raise falcon.HTTPBadRequest("Valid JSON is required", e.message)
            sid = self.process_new_registration(request_data)
            resp.body = json.dumps(sid)
            return falcon.HTTP_200
        else:
            raise falcon.HTTPBadRequest( "A valid JSON content-type is required", "please check the content type!")

    def process_new_registration(self, request_data):
        redis_connection = redis.Redis(connection_pool=self.redis_client)
        existing_cid_hash = redis_connection.hgetall(name=request_data["cid"])
        current_redis_timestamp = redis_connection.time()[0]
        if existing_cid_hash:
            if (int(existing_cid_hash["expiry_timestamp"]) > int(current_redis_timestamp)):
                sid = _generate_id()
                hset_sid = redis_connection.hsetnx(name=str(request_data["cid"]), key=sid, value=int(current_redis_timestamp))
                if hset_sid > 0:
                    return {"sid":sid}

redis_client = redis.ConnectionPool(host='localhost', port=6379, db=0)
es_client = Elasticsearch(hosts=[{"host":"localhost", "port":"9200"}])

registration = Registration(redis_client, es_client)

api = application = falcon.API()
api.add_route("/handshake", registration)
