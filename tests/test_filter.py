import unittest
from pseudonym.filter import IndexFilter
from pseudonym.models import Index


class TestIndexFilter(unittest.TestCase):
    def test_alias_filter(self):
        indexes = [Index(str(i), alias) for i, alias in enumerate(['a', 'b', 'c'])]
        indexes.append(Index('3', 'a'))
        res = IndexFilter(aliases=['a']).filter(indexes)
        self.assertEqual(len(res), 2)
        self.assertEqual({i.name for i in res}, {'0', '3'})

        res = IndexFilter(aliases=['b']).filter(indexes)
        self.assertEqual(len(res), 1)
        self.assertEqual({i.name for i in res}, {'1'})

    def test_slice(self):
        indexes = [Index(str(i), alias, routing=i) for i, alias in enumerate(['a', 'b', 'c'])]
        res = IndexFilter(slice=':2').filter(indexes)
        self.assertEqual(len(res), 2)
        self.assertEqual([i.name for i in res], ['2', '1'])

        res = IndexFilter(slice='2:').filter(indexes)
        self.assertEqual(len(res), 1)
        self.assertEqual([i.name for i in res], ['0'])

        indexes = [Index(str(i), alias, routing=-i) for i, alias in enumerate(['a', 'b', 'c'])]
        res = IndexFilter(slice=':2').filter(indexes)
        self.assertEqual(len(res), 2)
        self.assertEqual([i.name for i in res], ['0', '1'])
