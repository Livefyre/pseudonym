import logging

from elasticsearch.exceptions import RequestError


logger = logging.getLogger(__name__)


class SchemaEnforcer(object):
    def __init__(self, client):
        self.client = client

    def enforce(self, schema):
        for index in schema['indexes']:
            logger.info("Creating index %s" % index['name'])
            self.create_index(index)

        for alias in schema['aliases']:
            logger.info("Creating alias %s" % alias['name'])
            self.create_alias(alias)

    def create_index(self, index):
        body = {}
        if index.get('mappings'):
            body['mappings'] = index['mappings']

        if 'settings' in index:
            body['settings'] = index['settings']

        try:
            self.client.indices.create(index=index['name'], body=body)
            return
        except RequestError, e:
            if 'IndexAlreadyExistsException' not in e.error:
                raise

        if not index.get('mappings'):
            return

        for doc_type, mapping in index['mappings'].items():
            try:
                self.client.indices.put_mapping(index=index['name'], doc_type=doc_type, body={doc_type: mapping})
            except RequestError, e:
                if 'MergeMappingException' in e.error:
                    logger.exception("Error merging mappings")
                else:
                    raise

    def create_alias(self, alias):
        body = {key: alias[key] for key in ['routing', 'filter'] if key in alias}
        self.client.indices.put_alias(index=alias['indexes'], name=alias['name'], body=body)
