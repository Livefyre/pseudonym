import logging

from pseudonym.strategy import Strategies


logger = logging.getLogger(__name__)


class SchemaCompiler(object):
    @classmethod
    def compile(cls, existing, config):
        existing_aliases = {a['name']: a for a in existing.get('aliases', [])}
        schema = {'aliases': existing['aliases'][:],
                  'indexes': existing['indexes'][:]}

        has_diff = False
        index_link_args = []
        indexes_created = []

        for alias in config['aliases']:
            compiled_alias = {k: v for k, v in alias.items() if k not in {'settings', 'mappings', 'strategy'}}
            compiled_alias.setdefault('routing')
            compiled_alias.setdefault('filter')

            existing_alias = existing_aliases.get(compiled_alias['name'])

            if isinstance(alias['strategy'], basestring):
                strategy_type = alias['strategy']
                strategy_cfg = {}
            else:
                strategy_type = alias['strategy'].keys()[0]
                strategy_cfg = alias['strategy'][strategy_type]

            strategy = Strategies[strategy_type].instance()
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

            index_link_args.append((compiled_alias, strategy, strategy_cfg))

        for alias, strategy, strategy_cfg in index_link_args:
            existing = set([i['name'] for i in alias['indexes']])
            alias['indexes'] = [{'name': i['name'], 'alias': i['alias']} for i in strategy.link_indexes(schema, alias, strategy_cfg, indexes_created)]
            if existing != set([i['name'] for i in alias['indexes']]):
                has_diff = True

        if not has_diff:
            return None
        return schema
