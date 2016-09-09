import datetime
import unittest

from pseudonym.compiler import SchemaCompiler
from pseudonym.strategy import MonthlyRoutingStrategy


class TestSimplePointerStrategy(unittest.TestCase):
    def test(self):
        strategy = {'index_pointer': {'indexes': ['a', 'b']}}
        existing = {'aliases': [], 'indexes': [{'name': name, 'alias': 'something'} for name in ['a', 'b', 'c']]}
        cfg = {'aliases': [{'name': 'test', 'strategy': strategy}], 'templates': {'a': {'name': 'A'}}}
        schema = SchemaCompiler.compile(existing, cfg)

        test_schema = {'indexes': existing['indexes'],
                       'aliases': [{'name': 'test',
                                    'indexes': ['a', 'b'],
                                    'filter': None,
                                    'routing': None,
                                    'strategy': strategy}],
                       'templates': {'a': {'name': 'A'}},
                       'settings': []
                       }
        self.assertEqual(schema, test_schema)
        cfg['aliases'][0]['strategy']['index_pointer']['indexes'].append('c')
        test_schema['aliases'][0]['indexes'].append('c')
        schema = SchemaCompiler.compile(schema, cfg)
        self.assertEqual(schema, test_schema)
        self.assertIsNone(SchemaCompiler.compile(schema, cfg))


class TestAppendingPointerStrategy(unittest.TestCase):
    def test(self):
        strategy = {'appending_pointer': {'aliases': ['target']}}
        existing = {'aliases': [], 'indexes': [{'name': 'a', 'routing': 'a', 'alias': 'target'},
                                               {'name': 'b', 'routing': 'b', 'alias': 'something'}]}
        cfg = {'aliases': [{'name': 'test', 'strategy': strategy}]}
        schema = SchemaCompiler.compile(existing, cfg)

        test_schema = {'indexes': existing['indexes'],
                       'aliases': [{'name': 'test',
                                    'indexes': ['a'],
                                    'filter': None,
                                    'routing': None,
                                    'strategy': strategy}],
                       'templates': {}, 'settings': []
                       }
        self.assertEqual(schema, test_schema)
        cfg['aliases'].append({'name': 'target', 'strategy': 'single'})
        test_schema['indexes'].append({'alias': 'target', 'mappings': None, 'name': 'target', 'settings': None})
        test_schema['aliases'][0]['indexes'].append('target')
        schema = SchemaCompiler.compile(schema, cfg)
        self.assertEqual(schema, test_schema)
        self.assertIsNone(SchemaCompiler.compile(schema, cfg))

    def test_initial_slice(self):
        strategy = {'appending_pointer': {'aliases': ['target'], 'initial': '-1'}}
        existing = {'aliases': [], 'indexes': [{'name': name, 'routing': name, 'alias': 'target'} for name in ['a', 'b']]}
        cfg = {'aliases': [{'name': 'test', 'strategy': strategy}]}
        schema = SchemaCompiler.compile(existing, cfg)

        test_schema = {'indexes': existing['indexes'],
                       'aliases': [{'name': 'test',
                                    'indexes': ['b'],
                                    'filter': None,
                                    'routing': None,
                                    'strategy': strategy}],
                       'templates': {}, 'settings': []
                       }
        self.assertEqual(schema, test_schema)


class TestAliasPointerStrategy(unittest.TestCase):
    maxDiff = None

    def test(self):
        existing = {'aliases': [], 'indexes': [{'name': 'a', 'alias': 'target'}, {'name': 'b', 'alias': 'something'}]}
        strategy = {'alias_pointer': {'aliases': ['target']}}
        cfg = {'aliases': [{'name': 'test', 'strategy': strategy}]}
        schema = SchemaCompiler.compile(existing, cfg)

        test_schema = {'indexes': existing['indexes'],
                       'aliases': [{'name': 'test',
                                    'indexes': ['a'],
                                    'filter': None,
                                    'routing': None,
                                    'strategy': strategy}],
                       'templates': {}, 'settings': []
                       }

        self.assertEqual(schema, test_schema)
        cfg['aliases'].append({'name': 'target', 'strategy': 'single'})
        test_schema['indexes'].append({'alias': 'target', 'mappings': None, 'name': 'target', 'settings': None})
        test_schema['aliases'][0]['indexes'].append('target')
        schema = SchemaCompiler.compile(schema, cfg)
        self.assertEqual(schema, test_schema)
        self.assertIsNone(SchemaCompiler.compile(schema, cfg))

    # TODO: Test slicing


class TestDateRoutingStrategy(unittest.TestCase):
    def test(self):
        existing = {'aliases': [], 'indexes': []}
        strategy = {'date': {'indexes': {'201401': datetime.date(2014, 1, 1)}}}
        cfg = {'aliases': [{'name': 'test', 'strategy': strategy}]}
        schema = SchemaCompiler.compile(existing, cfg)
        test_schema = {'indexes': [{'alias': 'test', 'mappings': None, 'name': '201401', 'settings': None, 'routing': datetime.date(2014, 1, 1)}],
                       'aliases': [{'name': 'test',
                                    'indexes': ['201401'],
                                    'filter': None,
                                    'routing': None,
                                    'strategy': strategy}],
                       'templates': {}, 'settings': []
                       }

        self.assertEqual(schema, test_schema)
        cfg['aliases'][0]['strategy']['date']['indexes']['201402'] = datetime.date(2014, 2, 1)
        test_schema['indexes'].append({'alias': 'test', 'mappings': None, 'name': '201402', 'settings': None, 'routing': datetime.date(2014, 2, 1)})
        test_schema['aliases'][0]['indexes'].append('201402')
        schema = SchemaCompiler.compile(schema, cfg)
        self.assertEqual(schema, test_schema)
        self.assertIsNone(SchemaCompiler.compile(schema, cfg))


class TestMonthlyRoutingStrategy(unittest.TestCase):
    def test(self):
        existing = {'aliases': [], 'indexes': []}
        strategy = {'monthly': {'index_name_pattern': '%Y%m'}}
        cfg = {'aliases': [{'name': 'test', 'strategy': strategy}]}
        MonthlyRoutingStrategy.instance(MonthlyRoutingStrategy(lambda: datetime.date(2014, 1, 1)))
        schema = SchemaCompiler.compile(existing, cfg)
        test_schema = {'indexes': [{'alias': 'test', 'mappings': None, 'name': '201401', 'settings': None, 'routing': '2014-01-01T00:00:00'}],
                       'aliases': [{'name': 'test',
                                    'indexes': ['201401'],
                                    'filter': None,
                                    'routing': None,
                                    'strategy': strategy}],
                       'templates': {}, 'settings': []
                       }

        self.assertEqual(schema, test_schema)
        MonthlyRoutingStrategy.instance(MonthlyRoutingStrategy(lambda: datetime.date(2014, 2, 1)))
        test_schema['indexes'].append({'alias': 'test', 'mappings': None, 'name': '201402', 'settings': None, 'routing': '2014-02-01T00:00:00'})
        test_schema['aliases'][0]['indexes'].append('201402')
        schema = SchemaCompiler.compile(schema, cfg)
        self.assertEqual(schema, test_schema)
        self.assertIsNone(SchemaCompiler.compile(schema, cfg))


class TestSettings(unittest.TestCase):
    def test(self):
        indexes = [{'alias': 'alias_1', 'name': 'test_index_%s' % v, 'routing': v} for v in range(3)]
        existing = {'aliases': [{'name': 'alias_1', 'indexes': [index['name'] for index in indexes]}],
                    'indexes': indexes, 'templates': {}}
        cfg = {'settings': [{'filter': {'slice': ':2', 'aliases': ['alias_1']},
                             'settings': {"index.routing.allocation.require.storage_type": "a"}}],
               'aliases': []}

        schema = SchemaCompiler.compile(existing, cfg)
        test_schema = existing.copy()
        test_schema['settings'] = [{'indexes': ['test_index_2', 'test_index_1'],
                                    'settings': {"index.routing.allocation.require.storage_type": "a"}}]
        self.assertEqual(schema, test_schema)
