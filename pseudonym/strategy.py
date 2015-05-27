import datetime
from pseudonym.errors import InvalidConfigError
from pseudonym.errors import RoutingException


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


class RangeRouter(BaseRouter):
    def route(self, routing):
        for index in self.indexes:
            if routing >= index['routing']:
                return index
        if not index:
            raise RoutingException("%s has no indexes" % self.alias['name'])

        return index


class RoutingStrategy(object):
    uses_alias = True
    Router = None

    __instance = None

    @classmethod
    def instance(cls, inst=None):
        if inst:
            cls.__instance = inst
        if not cls.__instance:
            cls.__instance = cls()
        return cls.__instance

    def link_indexes(self, indexes, alias, cfg, new_indexes):
        indexes = alias['indexes'][:]
        for index in new_indexes:
            if index['alias'] == alias['name']:
                indexes.append(index)
        return indexes

    def list_indexes(self, schema, alias):
        return [i for i in schema['indexes'] if i['name'] in {i['name'] for i in alias['indexes']}]

    def get_router(self, schema, alias):
        if not self.Router:
            raise RoutingException("Cannot route to this %s." % alias['name'])
        return self.Router(self.list_indexes(schema, alias), alias)


@register('index_pointer')
class IndexPointerStrategy(RoutingStrategy):
    def create_indexes(self, schema, alias, cfg):
        return []

    def link_indexes(self, schema, alias, cfg, new_indexes):
        return [i for i in schema['indexes'] if i['name'] in set(cfg['indexes'])]


@register('appending_pointer')
class AppendingPointerStrategy(RoutingStrategy):
    def create_indexes(self, schema, alias, cfg):
        return []

    def link_indexes(self, schema, alias, cfg, new_indexes):
        indexes = alias['indexes'][:]

        for index in new_indexes:
            if index['alias'] in cfg['aliases']:
                indexes.append(index)

        return indexes


@register('alias_pointer')
class AliasPointerStrategy(RoutingStrategy):
    Router = RangeRouter

    def create_indexes(self, schema, alias, cfg):
        return []

    def link_indexes(self, schema, alias, cfg, new_indexes):
        indexes = [i for i in schema['indexes'] if i.get('alias') in cfg['aliases']]
        is_sorted = False
        try:
            indexes = sorted(indexes, key=lambda x: x['routing'], reverse=True)
            is_sorted = True
        except KeyError:
            pass

        if 'slice' in cfg:
            if not is_sorted:
                raise InvalidConfigError("Indexes must use routing to be sliced.")
            indexes = indexes[slice(*[int(k) if k else None for k in cfg['slice'].split(':')])]
        return indexes

    def list_indexes(self, schema, alias):
        indexes = super(AliasPointerStrategy, self).list_indexes(schema, alias)
        try:
            return sorted(indexes, key=lambda x: x['routing'], reverse=True)
        except KeyError:
            raise RoutingException("Cannot route to AliasPointerStrategy alias without routing parameter.")


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
        for index in indexes:
            index['routing'] = datetime.datetime.strptime(index['routing'], '%Y-%m-%dT%H:%M:%S')
        return sorted(indexes, key=lambda x: x['routing'], reverse=True)


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
        next_month = datetime.datetime(today.year + (1 if today.month == 12 else 0), (today.month + 1) % 12, 1)
        return {'name': next_month.strftime(cfg['index_name_pattern']), 'routing': next_month}
