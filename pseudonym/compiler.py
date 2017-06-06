import logging

from pseudonym.errors import InvalidConfigError
from pseudonym.strategy import Strategies
from pseudonym.filter import IndexFilter
from pseudonym.models import Index

logger = logging.getLogger(__name__)


class SchemaCompiler(object):
    @classmethod
    def add_index(cls, existing, alias_name, index_name, routing):
        for alias in existing['aliases']:
            if alias['name'] == alias_name:
                break
        else:
            raise Exception("Alias %s does not exist." % alias_name)

        strategy, strategy_cfg = cls._get_strategy(alias)

        for index in existing['indexes']:
            if index['name'] == index_name:
                break
        else:
            index = {'name': index_name, 
                     'alias': alias_name, 
                     'mappings': alias.get('mappings'), 
                     'settings': alias.get('settings')}
            existing['indexes'].append(index)

        if routing:
            index['routing'] = routing

        for alias in existing['aliases']:
            strategy, strategy_cfg = cls._get_strategy(alias)
            alias['indexes'] = strategy.link_indexes(existing, alias, strategy_cfg, [index])
        return existing

    @classmethod
    def _get_strategy(cls, alias_cfg):
        if isinstance(alias_cfg['strategy'], basestring):
            strategy_type = alias_cfg['strategy']
            strategy_cfg = {}
        else:
            strategy_type = alias_cfg['strategy'].keys()[0]
            strategy_cfg = alias_cfg['strategy'][strategy_type]

        return Strategies[strategy_type].instance(), strategy_cfg

    @classmethod
    def compile(cls, existing, config):
        existing_aliases = {a['name']: a for a in existing.get('aliases', [])}
        schema = {'aliases': existing['aliases'][:],
                  'indexes': existing['indexes'][:],
                  'templates': existing.get('templates', {})}

        has_diff = False
        existing_templates = schema['templates'].copy()
        schema['templates'].update(config.get('templates', {}))
        if schema['templates'] != existing_templates:
            has_diff = True

        index_link_args = []
        indexes_created = []

        for alias in config['aliases']:
            compiled_alias = {k: v for k, v in alias.items() if k not in {'settings', 'mappings'}}
            compiled_alias.setdefault('routing')
            compiled_alias.setdefault('filter')

            existing_alias = existing_aliases.get(compiled_alias['name'])

            strategy, strategy_cfg = cls._get_strategy(alias)
            if strategy.uses_alias and not (existing_alias and all([compiled_alias[k] == existing_alias[k] for k in ['filter', 'routing']])):
                has_diff = True

            if strategy.uses_alias and not existing_alias:
                schema['aliases'].append(compiled_alias)

            if existing_alias:
                existing_alias.update(compiled_alias)
                compiled_alias = existing_alias
            compiled_alias.setdefault('indexes', [])

            for index in strategy.create_indexes(schema, alias, strategy_cfg):
                indexes_created.append(index)
                has_diff = True
                index_cfg = index.copy()
                index_cfg['settings'] = alias.get('settings')
                index_cfg['mappings'] = alias.get('mappings')
                schema['indexes'].append(index_cfg)

            if strategy.uses_alias:
                index_link_args.append((compiled_alias, strategy, strategy_cfg))

        for alias, strategy, strategy_cfg in index_link_args:
            existing_indexes = set(alias['indexes'])
            indexes = strategy.link_indexes(schema, alias, strategy_cfg, indexes_created)
            if not indexes:
                raise InvalidConfigError("%s has no indexes" % alias['name'])
            alias['indexes'] = indexes

            if existing_indexes != set(alias['indexes']):
                has_diff = True

        settings = [cls.compile_settings(schema, s_config) for s_config in config.get('settings', [])]
        has_diff = has_diff or settings != existing.get('settings')

        if not has_diff:
            return None

        schema['settings'] = settings
        return schema

    @classmethod
    def compile_settings(cls, schema, s_config):
        index_filter = IndexFilter(**s_config['filter'])
        indexes = [Index(**i) for i in schema['indexes']]
        return {'indexes': [i.name for i in index_filter.filter(indexes)],
                'settings': s_config['settings']}
