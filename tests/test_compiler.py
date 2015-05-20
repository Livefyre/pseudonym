import datetime
import unittest

from pseudonym.compiler import SchemaCompiler
from pseudonym.strategy import MonthlyRoutingStrategy


class TestSimplePointerStrategy(unittest.TestCase):
    def test(self):
        existing = {'aliases': [], 'indexes': [{'name': name, 'alias': 'something'} for name in ['a', 'b', 'c']]}
        cfg = {'aliases': [{'name': 'test', 'strategy': {'index_pointer': {'indexes': ['a', 'b']}}}]}
        schema = SchemaCompiler.compile(existing, cfg)

        test_schema = {'indexes': existing['indexes'],
                       'aliases': [{'name': 'test',
                                    'indexes': [{'name': name, 'alias': 'something'} for name in ['a', 'b']],
                                    'filter': None,
                                    'routing': None}]
                       }
        self.assertEqual(schema, test_schema)
        cfg['aliases'][0]['strategy']['index_pointer']['indexes'].append('c')
        test_schema['aliases'][0]['indexes'].append({'name': 'c', 'alias': 'something'})
        schema = SchemaCompiler.compile(schema, cfg)
        self.assertEqual(schema, test_schema)
        self.assertIsNone(SchemaCompiler.compile(schema, cfg))


class TestAppendingPointerStrategy(unittest.TestCase):
    def test(self):
        existing = {'aliases': [], 'indexes': [{'name': 'a', 'alias': 'target'}, {'name': 'b', 'alias': 'something'}]}
        cfg = {'aliases': [{'name': 'test', 'strategy': {'appending_pointer': {'aliases': ['target']}}}]}
        schema = SchemaCompiler.compile(existing, cfg)

        test_schema = {'indexes': existing['indexes'],
                       'aliases': [{'name': 'test',
                                    'indexes': [],
                                    'filter': None,
                                    'routing': None}]
                       }
        self.assertEqual(schema, test_schema)
        cfg['aliases'].append({'name': 'target', 'strategy': 'single'})
        test_schema['indexes'].append({'alias': 'target', 'mappings': None, 'name': 'target', 'settings': None})
        test_schema['aliases'][0]['indexes'].append({'name': 'target', 'alias': 'target'})
        schema = SchemaCompiler.compile(schema, cfg)
        self.assertEqual(schema, test_schema)
        self.assertIsNone(SchemaCompiler.compile(schema, cfg))


class TestAliasPointerStrategy(unittest.TestCase):
    def test(self):
        existing = {'aliases': [], 'indexes': [{'name': 'a', 'alias': 'target'}, {'name': 'b', 'alias': 'something'}]}
        cfg = {'aliases': [{'name': 'test', 'strategy': {'alias_pointer': {'aliases': ['target']}}}]}
        schema = SchemaCompiler.compile(existing, cfg)

        test_schema = {'indexes': existing['indexes'],
                       'aliases': [{'name': 'test',
                                    'indexes': [{'name': 'a', 'alias': 'target'}],
                                    'filter': None,
                                    'routing': None}]
                       }
        self.assertEqual(schema, test_schema)
        cfg['aliases'].append({'name': 'target', 'strategy': 'single'})
        test_schema['indexes'].append({'alias': 'target', 'mappings': None, 'name': 'target', 'settings': None})
        test_schema['aliases'][0]['indexes'].append({'name': 'target', 'alias': 'target'})
        schema = SchemaCompiler.compile(schema, cfg)
        self.assertEqual(schema, test_schema)
        self.assertIsNone(SchemaCompiler.compile(schema, cfg))

    # TODO: Test slicing


class TestDateRoutingStrategy(unittest.TestCase):
    def test(self):
        existing = {'aliases': [], 'indexes': []}
        cfg = {'aliases': [{'name': 'test', 'strategy': {'date': {'indexes': {'201401': datetime.date(2014, 1, 1)}}}}]}
        schema = SchemaCompiler.compile(existing, cfg)
        test_schema = {'indexes': [{'alias': 'test', 'mappings': None, 'name': '201401', 'settings': None, 'routing': datetime.date(2014, 1, 1)}],
                       'aliases': [{'name': 'test',
                                    'indexes': [{'name': '201401', 'alias': 'test'}],
                                    'filter': None,
                                    'routing': None}]
                       }

        self.assertEqual(schema, test_schema)
        cfg['aliases'][0]['strategy']['date']['indexes']['201402'] = datetime.date(2014, 2, 1)
        test_schema['indexes'].append({'alias': 'test', 'mappings': None, 'name': '201402', 'settings': None, 'routing': datetime.date(2014, 2, 1)})
        test_schema['aliases'][0]['indexes'].append({'name': '201402', 'alias': 'test'})
        schema = SchemaCompiler.compile(schema, cfg)
        self.assertEqual(schema, test_schema)
        self.assertIsNone(SchemaCompiler.compile(schema, cfg))


class TestMonthlyRoutingStrategy(unittest.TestCase):
    def test(self):
        existing = {'aliases': [], 'indexes': []}
        cfg = {'aliases': [{'name': 'test', 'strategy': {'monthly': {'index_name_pattern': '%Y%m'}}}]}
        MonthlyRoutingStrategy.instance(MonthlyRoutingStrategy(lambda: datetime.date(2014, 1, 1)))
        schema = SchemaCompiler.compile(existing, cfg)
        test_schema = {'indexes': [{'alias': 'test', 'mappings': None, 'name': '201402', 'settings': None, 'routing': datetime.datetime(2014, 2, 1)}],
                       'aliases': [{'name': 'test',
                                    'indexes': [{'name': '201402', 'alias': 'test'}],
                                    'filter': None,
                                    'routing': None}]
                       }

        self.assertEqual(schema, test_schema)
        MonthlyRoutingStrategy.instance(MonthlyRoutingStrategy(lambda: datetime.date(2014, 2, 1)))
        test_schema['indexes'].append({'alias': 'test', 'mappings': None, 'name': '201403', 'settings': None, 'routing': datetime.datetime(2014, 3, 1)})
        test_schema['aliases'][0]['indexes'].append({'name': '201403', 'alias': 'test'})
        schema = SchemaCompiler.compile(schema, cfg)
        self.assertEqual(schema, test_schema)
        self.assertIsNone(SchemaCompiler.compile(schema, cfg))
