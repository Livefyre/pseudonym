import datetime
import unittest
import json

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
        cfg = {'aliases': [{'name': 'alias1', 'strategy': {'date': {'indexes': {'201401': datetime.date(2014, 1, 1).isoformat()}}}}]}
        self.manager.update(cfg)

        schema = self.client.get(index=self.test_schema_index, id='master')
        self.assertEqual(schema['_version'], 1)
        source = schema.pop('_source')
        schema_doc = json.loads(source.get('schema'))

        self.assertEqual({a['name'] for a in schema_doc['aliases']}, {'alias1'})
        self.assertEqual({i['name'] for i in schema_doc['indexes']}, {'201401'})

        cfg['aliases'][0]['strategy']['date']['indexes']['201402'] = datetime.date(2014, 2, 1).isoformat()
        self.manager.update(cfg)
        schema = self.client.get(index=self.test_schema_index, id='master')
        self.assertEqual(schema['_version'], 2)
        source = schema.pop('_source')
        schema_doc = json.loads(source.get('schema'))

        self.assertEqual({a['name'] for a in schema_doc['aliases']}, {'alias1'})
        self.assertEqual({i['name'] for i in schema_doc['indexes']}, {'201401', '201402'})

        cfg['aliases'].append({'name': 'alias2', 'strategy': {'date': {'indexes': {'201501': datetime.date(2015, 1, 1).isoformat()}}}})

        self.manager.update(cfg)
        schema = self.client.get(index=self.test_schema_index, id='master')
        self.assertEqual(schema['_version'], 3)
        source = schema.pop('_source')
        schema_doc = json.loads(source.get('schema'))

        self.assertEqual({a['name'] for a in schema_doc['aliases']}, {'alias1', 'alias2'})
        self.assertEqual({i['name'] for i in schema_doc['indexes']}, {'201401', '201402', '201501'})

    def test_add_index(self):
        cfg = {'aliases': [{'name': 'alias1', 'strategy': {'date': {'indexes': {'201401': datetime.date(2014, 1, 1).isoformat()}}}}]}
        self.manager.update(cfg)
        self.manager.add_index('alias1', '201402', datetime.date(2014, 1, 2).isoformat())
        schema = self.client.get(index=self.test_schema_index, id='master')
        source = schema.pop('_source')
        schema_doc = json.loads(source.get('schema'))

        for alias in schema_doc['aliases']:
            if alias['name'] == 'alias1':
                break
        self.assertIn('201402', alias['indexes'])
        self.assertIn('201402', [i['name'] for i in schema_doc['indexes']])

    def test_remove_index(self):
        cfg = {'aliases': [{'name': 'alias1', 'strategy': {'date': {'indexes': {'201501': datetime.date(2015, 1, 1).isoformat(), '201401': datetime.date(2014, 1, 1).isoformat()}}}}]}
        self.manager.update(cfg)
        self.manager.remove_index('201401')
        schema = self.client.get(index=self.test_schema_index, id='master')['_source']
        schema_doc = json.loads(schema.get('schema'))

        self.assertEqual(len(schema_doc['indexes']), 1)
        self.assertEqual(schema_doc['indexes'][0]['name'], '201501')
        self.assertEqual(len(schema_doc['aliases']), 1)
        self.assertEqual(schema_doc['aliases'][0]['indexes'], ['201501'])

    def test_reindex_cutover(self):
        source_index = "reindex_2017_01"
        # Add both indexes to aliases before cutover
        target_index = '%s_new' % source_index
        alias1 = 'cutover1'

        cfg = {'aliases': [{'name': alias1, 'strategy': {'date': {'indexes': {source_index: datetime.date(2017, 1, 1).isoformat()}}}}]}
        self.manager.update(cfg)

        _, schema = self.manager.get_current_schema(True)
        self.assertEquals(schema['aliases'][0]['name'], alias1)

        self.manager.reindex_cutover(source_index)

        _, schema = self.manager.get_current_schema(True)
        aliases = [alias for alias in schema['aliases'] if alias['name'] is alias1]
        for alias in aliases:
            self.assertTrue(target_index in alias['indexes'])
            self.assertTrue(source_index not in alias['indexes'])
