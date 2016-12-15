'''
forked from mentzera --> from AWS/Twitter/ES tutorial
'''
from elasticsearch import Elasticsearch
import config
from elasticsearch.exceptions import ElasticsearchException
from tweet_utils import get_tweet, id_field, tweet_mapping


index_name = 'teamkeywords'
doc_type = 'tweet'
mapping = {doc_type: tweet_mapping}
bulk_chunk_size = config.es_bulk_chunk_size


def create_index(es, index_name, mapping):
    print('creating index {}...'.format(index_name))
    es.indices.create(index_name, body={'mappings': mapping})


def check_index():
    es = Elasticsearch(host=config.es_host, port=config.es_port)
    if es.indices.exists(index_name):
        print ('index {} already exists'.format(index_name))
        try:
            es.indices.put_mapping(doc_type, tweet_mapping, index_name)
        except ElasticsearchException as e:
            print('error putting mapping:\n' + str(e))
            print('deleting index {}...'.format(index_name))
            es.indices.delete(index_name)
            create_index(es, index_name, mapping)
    else:
        print('index {} does not exist'.format(index_name))
        create_index(es, index_name, mapping)


def load(doc):
    es = Elasticsearch(host=config.es_host, port=config.es_port)
    tweet = get_tweet(doc)
    result = es.index(index=index_name, doc_type=doc_type,
                   id=tweet[id_field], body=tweet)
    return result
