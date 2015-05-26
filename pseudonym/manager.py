import logging

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
            self._schema = schema, source
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
            self._strategies = {a['name']: Strategies[a['strategy']].instance() for a in schema['aliases']}
        return self._strategies

    def update(self, config):
        if not self.client.indices.exists(index=self.schema_index):
            self.client.indices.create(index=self.schema_index)
            self.client.index(index=self.schema_index, id='master', doc_type=self.schema_type,
                              body={'aliases': [], 'indexes': []}, version=0, version_type='external')

        meta, existing = self.get_current_schema(True)
        schema = SchemaCompiler.compile(existing, config)
        if schema is None:
            return
        self.client.index(index=self.schema_index, doc_type=self.schema_type,
                          id='master', body=schema,
                          version=meta['_version'] + 1, version_type='external')
        self.client.create(index=self.schema_index, doc_type=self.schema_type, id=meta['_version'] + 1, body=schema)
        self._routers = {}

    def enforce(self):
        SchemaEnforcer(self.client).enforce(self.get_current_schema(True)[1])

    def route(self, alias, routing):
        return self.get_router(alias).route(routing)

    def reload(self):
        self.get_current_schema(True)
