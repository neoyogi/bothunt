import falcon
import json
import uuid
import redis
import datetime
from elasticsearch import Elasticsearch
import elasticsearch

def _generate_id():
    return "z_"+str(uuid.uuid4())[:8]

class Registration(object):

    def __init__(self, redis_client, es_client):
        self.redis_client = redis_client
        self.es_client = es_client

    def on_post(self, req, resp, reg_type):
        """Handle for POST request to register a new customer or renew an existing customer"""
        if reg_type == "new":
            self.request_type = "new"
        elif reg_type == "renew":
            self.request_type = "renew"
        if req.content_type and req.content_type.lower() == "application/json":
            body = req.stream.read()
            if not body:
                raise falcon.HTTPBadRequest("Valid data in the body field is required", "No data in the body!")
            else:
                try:
                    request_data = json.loads(body)
                except Exception as e:
                    raise falcon.HTTPBadRequest("Valid JSON is required", e.message)
                registration_information = self.process_registration_and_persist_to_redis_elasticsearch(request_data)
                print registration_information
                resp.body = json.dumps(registration_information)
                return falcon.HTTP_200
        else:
            raise falcon.HTTPBadRequest( "A valid JSON content-type is required", "please check the content type!")

    def process_registration_and_persist_to_redis_elasticsearch(self, request_data):
        if self.request_type == "new":
            redis_connection = redis.Redis(connection_pool=self.redis_client)
            CID = _generate_id()
            request_data["timestamp"] = int(redis_connection.time()[0])
            request_data["expiry_timestamp"] = int(((datetime.datetime.fromtimestamp(request_data["timestamp"]) + datetime.timedelta(days=request_data["subscription"]))-datetime.datetime(1970,1,1)).total_seconds())
            redis_connection.hmset(name=CID, mapping=request_data)
            redis_connection.expireat(name=CID, when=request_data["expiry_timestamp"])
            self.es_client.create(index="registration",doc_type="license",id=CID, body=request_data)
            return {CID:request_data}
        elif self.request_type == "renew":
            redis_connection = redis.Redis(connection_pool=self.redis_client)
            existing_registration_json = redis_connection.hgetall(name=request_data["cid"])
            if "subscription" in request_data.keys() and "cid" in request_data.keys():
                new_expiry_timestamp = int(((datetime.datetime.fromtimestamp(float(existing_registration_json["expiry_timestamp"])) + datetime.timedelta(days=request_data["subscription"]))-datetime.datetime(1970,1,1)).total_seconds())
                redis_connection.hset(name=request_data["cid"], key="expiry_timestamp", value=new_expiry_timestamp)
                redis_connection.hset(name=request_data["cid"], key="subscription", value=int(int(existing_registration_json["subscription"])+int(request_data["subscription"])))
                redis_connection.persist(name=request_data["cid"])
                # redis_connection.expireat(name=request_data["cid"], when=new_expiry_timestamp)
                # new_timestamp = int(redis_connection.time()[0])
                # redis_connection.hset(name=request_data["cid"], key="timestamp", value=new_timestamp)
                new_registration_json = redis_connection.hgetall(name=request_data["cid"])

                try:
                    self.es_client.create(index="registration",doc_type="license",id=request_data["cid"], body=new_registration_json)
                except elasticsearch.ConflictError as e:
                    self.es_client.delete(index="registration",doc_type="license",id=request_data["cid"])
                    self.es_client.create(index="registration",doc_type="license",id=request_data["cid"], body=new_registration_json)
                return {request_data["cid"]:new_registration_json}

redis_client = redis.ConnectionPool(host='localhost', port=6379, db=0)
es_client = Elasticsearch(hosts=[{"host":"localhost", "port":"9200"}])
customer_registration = Registration(redis_client, es_client)

api = application = falcon.API()
api.add_route("/registration/{reg_type}", customer_registration)
