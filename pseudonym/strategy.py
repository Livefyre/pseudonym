import datetime
from pseudonym.errors import InvalidConfigError
from pseudonym.errors import RoutingException
from pseudonym.filter import IndexFilter
from pseudonym.models import Index
from pseudonym.filter import str_to_slice


Strategies = {}


def register(name):
    def dec(cls):
        Strategies[name] = cls
        return cls
    return dec


class BaseRouter(object):
    def __init__(self, indexes, alias):
        self.indexes = indexes
        self.alias = alias

    def route(self, routing):
        raise NotImplementedError()


class AliasRouter(BaseRouter):
    def route(self, _):
        return self.alias


class RangeRouter(BaseRouter):
    def route(self, routing):
        for index in self.indexes:
            if routing >= index['routing']:
                return index
        if not index:
            raise RoutingException("%s has no indexes" % self.alias['name'])

        return index


_INST = {}


class RoutingStrategy(object):
    uses_alias = True
    Router = None

    __instance = None

    @classmethod
    def instance(cls, inst=None):
        if inst:
            _INST[cls] = inst
        elif cls not in _INST:
            _INST[cls] = cls()
        return _INST[cls]

    def link_indexes(self, indexes, alias, cfg, new_indexes):
        indexes = alias['indexes'][:]
        for index in new_indexes:
            if index['alias'] == alias['name']:
                indexes.append(index['name'])
        return indexes

    def list_indexes(self, schema, alias):
        return [i for i in schema['indexes'] if i['name'] in set(alias['indexes'])]

    def get_router(self, schema, alias):
        if not self.Router:
            raise RoutingException("Cannot route to this %s." % alias['name'])
        return self.Router(self.list_indexes(schema, alias), alias)


@register('index_pointer')
class IndexPointerStrategy(RoutingStrategy):
    def create_indexes(self, schema, alias, cfg):
        return []

    def link_indexes(self, schema, alias, cfg, new_indexes):
        return [i['name'] for i in schema['indexes'] if i['name'] in set(cfg['indexes'])]


@register('appending_pointer')
class AppendingPointerStrategy(RoutingStrategy):
    def create_indexes(self, schema, alias, cfg):
        return []

    def link_indexes(self, schema, alias, cfg, new_indexes):
        indexes = set(alias['indexes'][:])

        if not indexes:
            indexes = self._get_initial(schema, alias, cfg)

        for index in new_indexes:
            if index['alias'] in cfg['aliases']:
                indexes.add(index['name'])

        return list(indexes)

    def _get_initial(self, schema, alias, cfg):
        indexes = [index for index in schema['indexes'] if index['alias'] in cfg['aliases']]

        try:
            indexes = sorted(indexes, key=lambda x: x['routing'], reverse=True)
        except KeyError:
            raise InvalidConfigError("Appending pointer targets must have routing.")

        if 'initial' in cfg:
            indexes = indexes[str_to_slice(cfg['initial'])]
        return set([i['name'] for i in indexes])


@register('alias_pointer')
class AliasPointerStrategy(RoutingStrategy):
    Router = RangeRouter

    def create_indexes(self, schema, alias, cfg):
        return []

    def link_indexes(self, schema, alias, cfg, new_indexes):
        index_filter = IndexFilter(aliases=cfg['aliases'], slice=cfg.get('slice'))
        return [index.name for index in index_filter.filter([Index(**i) for i in schema['indexes']])]

    def list_indexes(self, schema, alias):
        indexes = super(AliasPointerStrategy, self).list_indexes(schema, alias)
        try:
            return sorted(indexes, key=lambda x: x['routing'], reverse=True)
        except KeyError:
            raise RoutingException("Cannot route to AliasPointerStrategy alias without routing parameter.")


@register('latest_index')
class LatestIndexStrategy(AliasPointerStrategy):
    Router = AliasRouter

    def link_indexes(self, schema, alias, cfg, new_indexes):
        cfg = cfg.copy()
        cfg['slice'] = ':1'
        return super(LatestIndexStrategy, self).link_indexes(schema, alias, cfg, new_indexes)


@register('single')
class SingleIndexRoutingStrategy(RoutingStrategy):
    uses_alias = False

    class Router(BaseRouter):
        def route(self, routing):
            return self.indexes[0]

    def create_indexes(self, schema, alias, cfg):
        if alias['name'] not in {i['name'] for i in schema['indexes']}:
            return [{'name': alias['name'], 'alias': alias['name']}]
        return []

    def list_indexes(self, schema, alias):
        return [{'name': alias['name'], 'alias': alias['name']}]


@register('date')
class DateRoutingStrategy(RoutingStrategy):
    Router = RangeRouter

    def create_indexes(self, schema, alias, cfg):
        existing = {i['name'] for i in schema['indexes']}
        return [{'name': name, 'routing': routing, 'alias': alias['name']} for name, routing in cfg['indexes'].items() if name not in existing]

    def list_indexes(self, schema, alias):
        indexes = super(DateRoutingStrategy, self).list_indexes(schema, alias)
        adapted_indexes = []
        for index in indexes:
            adapted_index = index.copy()
            adapted_index['routing'] = datetime.datetime.strptime(index['routing'], '%Y-%m-%dT%H:%M:%S')
            adapted_indexes.append(adapted_index)
        return sorted(adapted_indexes, key=lambda x: x['routing'], reverse=True)


class CalendarRoutingStrategy(DateRoutingStrategy):
    def __init__(self, today=datetime.date.today):
        self.today = today

    def create_indexes(self, schema, alias, cfg):
        next_index = self.get_next(cfg)
        existing = {i['name'] for i in schema['indexes']}
        if next_index['name'] not in existing:
            next_index['alias'] = alias['name']
            return [next_index]
        return []


@register('monthly')
class MonthlyRoutingStrategy(CalendarRoutingStrategy):
    def get_next(self, cfg):
        today = self.today()
        this_month = datetime.datetime(today.year, today.month, 1)
        return {'name': this_month.strftime(cfg['index_name_pattern']), 'routing': this_month.isoformat()}


@register('annual')
class AnnualRoutingStrategy(CalendarRoutingStrategy):
    def get_next(self, cfg):
        today = self.today()
        this_year = datetime.datetime(today.year, 1, 1)
        return {'name': this_year.strftime(cfg['index_name_pattern']), 'routing': this_year.isoformat()}
