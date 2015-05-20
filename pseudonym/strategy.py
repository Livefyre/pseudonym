import datetime
from pseudonym.errors import InvalidConfigError


class RoutingException(Exception):
    pass


Strategies = {}


def register(name):
    def dec(cls):
        Strategies[name] = cls
        return cls
    return dec


class RoutingStrategy(object):
    uses_alias = True

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


@register('index_pointer')
class IndexPointerStrategy(RoutingStrategy):
    def create_indexes(self, schema, alias, cfg):
        return []

    def link_indexes(self, schema, alias, cfg, new_indexes):
        return [i for i in schema['indexes'] if i['name'] in set(cfg['indexes'])]

    def route(self, routing):
        raise RoutingException("Cannot route to this alias.")


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

    def route(self, routing):
        raise RoutingException("Cannot route to this alias.")


@register('alias_pointer')
class AliasPointerStrategy(RoutingStrategy):
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


@register('single')
class SingleIndexRoutingStrategy(RoutingStrategy):
    uses_alias = False

    def create_indexes(self, schema, alias, cfg):
        if alias['name'] not in {i['name'] for i in schema['indexes']}:
            return [{'name': alias['name'], 'alias': alias['name']}]
        return []

    def link_indexes(self, schema, alias, cfg, new_indexes):
        return []

    def route(self, schema):
        return self.schema['config']['index']


@register('date')
class DateRoutingStrategy(RoutingStrategy):
    def create_indexes(self, schema, alias, cfg):
        existing = {i['name'] for i in schema['indexes']}
        return [{'name': name, 'routing': routing, 'alias': alias['name']} for name, routing in cfg['indexes'].items() if name not in existing]

    def route(self, routing):
        for index in self.indexes:
            if routing >= index['routing']:
                return index
        if not index:
            # Route map should be defined with at least one index.
            assert False
        # Return last index in route map.
        return index


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
