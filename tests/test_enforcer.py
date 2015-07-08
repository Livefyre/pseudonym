import unittest
from elasticsearch.client import Elasticsearch
from pseudonym.enforcer import SchemaEnforcer


class TestEnforcer(unittest.TestCase):
    def setUp(self):
        self.client = Elasticsearch()

    def tearDown(self):
        for index in ['test_index_1', 'test_index_2']:
            try:
                self.client.indices.delete(index=index)
            except:
                pass

    def test(self):
        SchemaEnforcer(self.client).enforce({'indexes': [{'name': 'test_index_1'},
                                                         {'name': 'test_index_2'}],
                                             'aliases': [{'name': 'test_alias_1', 'indexes': ['test_index_1'], 'routing': 'routing', 'filter': {'term': {'field1': 'val1'}}}],
                                             'templates': {'test_alias_1': {'template': 'test_index*', 'mappings': {'test_type': {'properties': {'field1': {'type': 'string'}}}}}}})

        test_index_1 = self.client.indices.get('test_index_1')['test_index_1']
        self.assertIn('test_alias_1', test_index_1['aliases'])
        alias = test_index_1['aliases']['test_alias_1']
        self.assertEqual(alias['filter'], {u'term': {u'field1': u'val1'}})
        self.assertEqual(alias['search_routing'], 'routing')
        self.assertEqual(alias['index_routing'], 'routing')
        self.assertEqual(test_index_1['mappings'], {u'test_type': {u'properties': {u'field1': {u'type': u'string'}}}})
        test_index_2 = self.client.indices.get('test_index_2')
        self.assertNotIn('aliases', test_index_2)

    def test_reassociate_alias(self):
        schema = {'indexes': [{'name': 'test_index_1', 'mappings': {'test_type': {'properties': {'field1': {'type': 'string'}}}}},
                              {'name': 'test_index_2'}],
                  'aliases': [{'name': 'test_alias_1', 'indexes': ['test_index_1']}],
                  'templates': {'test_alias_1': {'template': 'test_index*', 'mappings': {'test_type': {'properties': {'field1': {'type': 'string'}}}}}}}
        SchemaEnforcer(self.client).enforce(schema)
        test_index_1 = self.client.indices.get('test_index_1')['test_index_1']
        self.assertIn('test_alias_1', test_index_1['aliases'])
        test_index_2 = self.client.indices.get('test_index_2')
        self.assertNotIn('aliases', test_index_2)

        schema['aliases'][0]['indexes'][0] = 'test_index_2'
        SchemaEnforcer(self.client).enforce(schema)
        test_index_1 = self.client.indices.get('test_index_1')['test_index_1']
        self.assertNotIn('test_alias_1', test_index_1.get('aliases', []))
        test_index_2 = self.client.indices.get('test_index_2')['test_index_2']
        self.assertIn('test_alias_1', test_index_2['aliases'])
