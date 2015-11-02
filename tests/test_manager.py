import datetime
import unittest

from elasticsearch.client import Elasticsearch
from pseudonym.manager import SchemaManager


class TestSchemaManager(unittest.TestCase):
    test_schema_index = 'test_pseudonym'

    def setUp(self):
        self.client = Elasticsearch()
        self.manager = SchemaManager(self.client, schema_index=self.test_schema_index)

    def tearDown(self):
        self.client.indices.delete(self.test_schema_index)

    def test_schema_compiling(self):
        cfg = {'aliases': [{'name': 'alias1', 'strategy': {'date': {'indexes': {'201401': datetime.date(2014, 1, 1)}}}}]}
        self.manager.update(cfg)

        schema = self.client.get(index=self.test_schema_index, id='master')
        self.assertEqual(schema['_version'], 1)
        self.assertEqual({a['name'] for a in schema['_source']['aliases']}, {'alias1'})
        self.assertEqual({i['name'] for i in schema['_source']['indexes']}, {'201401'})

        cfg['aliases'][0]['strategy']['date']['indexes']['201402'] = datetime.date(2014, 2, 1)
        self.manager.update(cfg)
        schema = self.client.get(index=self.test_schema_index, id='master')
        self.assertEqual(schema['_version'], 2)
        self.assertEqual({a['name'] for a in schema['_source']['aliases']}, {'alias1'})
        self.assertEqual({i['name'] for i in schema['_source']['indexes']}, {'201401', '201402'})

        cfg['aliases'].append({'name': 'alias2', 'strategy': {'date': {'indexes': {'201501': datetime.date(2015, 1, 1)}}}})

        self.manager.update(cfg)
        schema = self.client.get(index=self.test_schema_index, id='master')
        self.assertEqual(schema['_version'], 3)
        self.assertEqual({a['name'] for a in schema['_source']['aliases']}, {'alias1', 'alias2'})
        self.assertEqual({i['name'] for i in schema['_source']['indexes']}, {'201401', '201402', '201501'})

    def test_add_index(self):
        cfg = {'aliases': [{'name': 'alias1', 'strategy': {'date': {'indexes': {'201401': datetime.date(2014, 1, 1)}}}}]}
        self.manager.update(cfg)
        self.manager.add_index('alias1', '201402', datetime.date(2014, 1, 2).isoformat())
        schema = self.client.get(index=self.test_schema_index, id='master')

        for alias in schema['_source']['aliases']:
            if alias['name'] == 'alias1':
                break
        self.assertIn('201402', alias['indexes'])
        self.assertIn('201402', [i['name'] for i in schema['_source']['indexes']])

    def test_remove_index(self):
        cfg = {'aliases': [{'name': 'alias1', 'strategy': {'date': {'indexes': {'201501': datetime.date(2015, 1, 1), '201401': datetime.date(2014, 1, 1)}}}}]}
        self.manager.update(cfg)
        self.manager.remove_index('201401')
        schema = self.client.get(index=self.test_schema_index, id='master')['_source']
        self.assertEqual(len(schema['indexes']), 1)
        self.assertEqual(schema['indexes'][0]['name'], '201501')
        self.assertEqual(len(schema['aliases']), 1)
        self.assertEqual(schema['aliases'][0]['indexes'], ['201501'])
