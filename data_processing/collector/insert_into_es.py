from elasticsearch import Elasticsearch
from elasticsearch import helpers

es = Elasticsearch()
actions = []

for i in range(1,1000):
    action = {
        "_index": "z_ab204a6d.20.03.2016",
        "_type":"logs",
        "_source":{
            "counter": str(i)
        }
    }

    actions.append(action)

if len(actions) > 0:
    helpers.bulk(es, actions)
