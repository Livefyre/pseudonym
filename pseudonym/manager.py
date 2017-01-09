import logging
import json

from elasticsearch.exceptions import NotFoundError
from pseudonym.compiler import SchemaCompiler
from pseudonym.enforcer import SchemaEnforcer
from pseudonym.errors import RoutingException
from pseudonym.strategy import Strategies


logger = logging.getLogger(__name__)


class SchemaManager(object):
    def __init__(self, client, schema_index='pseudonym'):
        self.client = client
        self.schema_index = schema_index
        self._schema = None
        self._strategies = None
        self._routers = {}

    schema_type = 'schema'

    def get_current_schema(self, force=False):
        if not self._schema or force:
            self._strategies = None
            schema = self.client.get(index=self.schema_index, id='master')
            source = schema.pop('_source')
            schema_doc = source.get('schema', source)
            if isinstance(schema_doc, basestring):
                schema_doc = json.loads(schema_doc)
            self._schema = schema, schema_doc
        return self._schema

    def get_router(self, alias_name):
        if alias_name not in self._routers:
            _, schema = self.get_current_schema()
            for alias in schema['aliases']:
                if alias['name'] == alias_name:
                    break
            else:
                raise RoutingException("%s is not in the schema." % alias_name)

            self._routers[alias_name] = self.strategies[alias_name].get_router(schema, alias)
        return self._routers[alias_name]

    @property
    def strategies(self):
        _, schema = self.get_current_schema()
        if not self._strategies:
            self._strategies = {a['name']: Strategies[a['strategy'].keys()[0]].instance() for a in schema['aliases']}
        return self._strategies

    def update(self, config):
        if not self.client.indices.exists(index=self.schema_index):
            self.client.indices.create(index=self.schema_index)
            schema = {'schema': json.dumps({'aliases': [], 'indexes': []})}
            self.client.index(index=self.schema_index, id='master', doc_type=self.schema_type,
                              body=schema, version=0, version_type='external')

        meta, existing = self.get_current_schema(True)
        schema = SchemaCompiler.compile(existing, config)
        if schema is None:
            return
        self.apply(meta, schema)

    def apply(self, meta, schema):
        schema_doc = {'schema': json.dumps(schema)}
        self.client.index(index=self.schema_index, doc_type=self.schema_type,
                          id='master', body=schema_doc, refresh=True,
                          version=meta['_version'] + 1, version_type='external')
        self.client.create(index=self.schema_index, doc_type=self.schema_type, id=meta['_version'] + 1, body=schema_doc)
        self._routers = {}

    def add_index(self, alias_name, index_name, routing=None):
        meta, schema = self.get_current_schema(True)
        for alias in schema['aliases']:
            if alias['name'] == alias_name:
                break
        else:
            raise Exception("Alias %s does not exist." % alias_name)

        for index in schema['indexes']:
            if index['name'] == index_name:
                break
        else:
            index_cfg = {'name': index_name, 'alias': alias_name, 'mappings': None, 'settings': None}
            if routing:
                index_cfg['routing'] = routing
            schema['indexes'].append(index_cfg)

        if index_name not in alias['indexes']:
            alias['indexes'].append(index_name)

        self.apply(meta, schema)

    def remove_index(self, index_name):
        meta, schema = self.get_current_schema(True)
        for alias in schema['aliases']:
            if index_name in alias['indexes']:
                alias['indexes'].remove(index_name)
        for index_cfg in schema['indexes'][:]:
            if index_cfg['name'] == index_name:
                schema['indexes'].remove(index_cfg)

        self.apply(meta, schema)

    def enforce(self):
        SchemaEnforcer(self.client).enforce(self.get_current_schema(True)[1])

    def route(self, alias, routing):
        return self.get_router(alias).route(routing)['name']

    def reload(self):
        self.get_current_schema(True)
