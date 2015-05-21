import datetime
import unittest

from pseudonym.errors import RoutingException
from pseudonym.strategy import Strategies


class TestNoRouting(unittest.TestCase):
    def test(self):
        for strategy in ['index_pointer', 'appending_pointer']:
            self.assertRaises(RoutingException, Strategies[strategy].instance().get_router, {}, {'name': 'alias1'})


class TestDateStrategyRouting(unittest.TestCase):
    def test(self):
        schema = {'aliases': [{'name': 'alias1', 'indexes': [{'name': '201401'}, {'name': '201402'}]}],
                  'indexes': [{'name': '201401', 'routing': datetime.datetime(2014, 1, 1)},
                              {'name': '201402', 'routing': datetime.datetime(2014, 2, 1)}]}
        router = Strategies['date'].instance().get_router(schema, schema['aliases'][0])
        self.assertEqual(router.route(datetime.datetime(2013, 12, 1))['name'], '201401')
        self.assertEqual(router.route(datetime.datetime(2014, 1, 1))['name'], '201401')
        self.assertEqual(router.route(datetime.datetime(2014, 2, 1))['name'], '201402')


class TestSingleIndexRoutingStrategy(unittest.TestCase):
    def test(self):
        schema = {'indexes': [{'name': 'test'}]}
        router = Strategies['single'].instance().get_router(schema, {'name': 'test'})
        self.assertEqual(router.route('who cares'), {'alias': 'test', 'name': 'test'})
