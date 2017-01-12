import unittest
from elasticsearch.client import Elasticsearch
from pseudonym.reindexer import Reindexer
from pseudonym.manager import SchemaManager
from elasticsearch.helpers import bulk

class TestReindexer(unittest.TestCase):
    def setUp(self):
        self.source_index = "reindex"
        self.target_index = "reindex_new"
        self.client = Elasticsearch()
        self.reindexer = Reindexer(self.client)
        self.schema_manager = SchemaManager(self.client)

        # try:
        #     read_only_setting = {"index": {"blocks": {"read_only": False}}}
        #     self.client.indices.put_settings(index=self.source_index, body=read_only_setting)
        # except:
        #     pass

        self.client.indices.create(index=self.source_index)

    def tearDown(self):
        for index in [self.source_index, self.target_index]:
            try:
                self.client.indices.delete(index=index)
            except:
                pass

    def test_reindex(self):
        create = []
        for i in ['a', 'b', 'c', 'd', 'e']:
            doc = {
                '_op_type': 'create',
                '_index': self.source_index,
                '_type': 'document',
                'doc': {'name': i}
            }
            create.append(doc)
        bulk(self.client, create, refresh=True)
        docs = self.client.search(index=self.source_index)
        self.assertEqual(len(docs['hits']['hits']), 5)

        self.reindexer.do_reindex(self.source_index, self.target_index, 3)

        self.client.indices.refresh(','.join([self.source_index, self.target_index]))
        docs = self.client.search(index=self.source_index)
        self.assertEqual(len(docs['hits']['hits']), 0)
        docs = self.client.search(index=self.target_index)
        self.assertEqual(len(docs['hits']['hits']), 5)


