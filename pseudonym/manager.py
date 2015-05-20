import logging

from elasticsearch.exceptions import NotFoundError
from pseudonym.compiler import SchemaCompiler
from pseudonym.enforcer import SchemaEnforcer
from pseudonym.strategy import Strategies


logger = logging.getLogger(__name__)


class SchemaManager(object):
    def __init__(self, client, schema_index='pseudonym'):
        self.client = client
        self.schema_index = schema_index
        self._schema = None
        self._strategies = None

    schema_type = 'schema'

    def get_current_schema(self, force=False):
        if not self._schema or force:
            self._strategies = None
            try:
                schema = self.client.get(index=self.schema_index, id='master')
                source = schema.pop('_source')
                self._schema = schema, source
            except NotFoundError:
                self.client.index(index=self.schema_index, id='master', doc_type=self.schema_type,
                                  body={'aliases': [], 'indexes': []}, version=0, version_type='external')
                self._schema = ({'_version': 0}, {'aliases': [], 'indexes': []})
        return self._schema

    @property
    def strategies(self):
        _, schema = self.get_current_schema()
        if not self._strategies:
            self._strategies = {a['name']: Strategies[a['strategy']].instance() for a in schema['aliases']}
        return self._strategies

    def update(self, config):
        if not self.client.indices.exists(self.schema_index):
            self.client.indices.create(index=self.schema_index)
        meta, existing = self.get_current_schema(True)
        schema = SchemaCompiler.compile(existing, config)
        if schema is None:
            return
        self.client.index(index=self.schema_index, doc_type=self.schema_type,
                          id='master', body=schema,
                          version=meta['_version'] + 1, version_type='external')
        self.client.create(index=self.schema_index, doc_type=self.schema_type, id=meta['_version'] + 1, body=schema)

    def enforce(self):
        SchemaEnforcer(self.client).enforce(self.get_current_schema(True)[1])

    def route(self, alias, routing):
        return self.strategies[alias].route(alias, routing)
