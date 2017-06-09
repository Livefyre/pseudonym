import logging

from elasticsearch.exceptions import RequestError, NotFoundError


logger = logging.getLogger(__name__)


class SchemaEnforcer(object):
    def __init__(self, client):
        self.client = client

    def enforce(self, schema):
        for name, template in schema['templates'].items():
            logger.info("Creating template %s" % name)
            self.client.indices.put_template(name=name, body=template)

        for index in schema['indexes']:
            logger.info("Creating index %s" % index['name'])
            self.create_index(index)

        for alias in schema['aliases']:
            logger.info("Creating alias %s" % alias['name'])
            self.create_alias(alias)

        for setting_cfg in schema['settings']:
            self.apply_settings(setting_cfg)

    def create_index_by_name(self, index_name):
        # By default ES will enforce whatever mapping mappings and settings are in the SI lib, IF the index name matches a template
        try:
            self.client.indices.create(index=index_name)
            return
        except RequestError, e:
            # index_already_exists_exception was introduced in ES 2.x
            if 'index_already_exists_exception' not in e.error and 'IndexAlreadyExistsException' not in e.error:
                raise

    def create_index(self, index):
        body = {}
        if index.get('mappings'):
            body['mappings'] = index['mappings']

        if index.get('settings'):
            body['settings'] = index['settings']

        try:
            self.client.indices.create(index=index['name'], body=body)
            return
        except RequestError, e:
            # index_already_exists_exception was introduced in ES 2.x
            if 'index_already_exists_exception' not in e.error and 'IndexAlreadyExistsException' not in e.error:
                raise

        if not index.get('mappings'):
            return

        for doc_type, mapping in index['mappings'].items():
            try:
                self.client.indices.put_mapping(index=index['name'], doc_type=doc_type, body={doc_type: mapping})
            except RequestError, e:
                # MergeMappingException is ES 1x
                if 'illegal_argument_exception' in e.error or 'MergeMappingException' in e.error:
                    logger.exception("Error merging mappings")
                else:
                    raise

    def create_alias(self, alias):
        existing = set()
        try:
            existing.update(self.client.indices.get_alias(name=alias['name']))
        except NotFoundError:
            pass

        actions = []

        for index in alias['indexes']:
            if index in existing:
                existing.discard(index)
                continue
            body = {'index': index, 'alias': alias['name']}
            body.update({key: alias[key] for key in ['routing', 'filter'] if key in alias})
            actions.append({'add': body})
        for index in existing:
            actions.append({'remove': {'index': index, 'alias': alias['name']}})

        if actions:
            self.client.indices.update_aliases({'actions': actions})

    def apply_settings(self, setting_cfg):
        print 'putting settings to %s, body=%s' % (','.join(setting_cfg['indexes']), setting_cfg['settings'])
        self.client.indices.put_settings(index=','.join(setting_cfg['indexes']), body=setting_cfg['settings'])

    def index_exists(self, index):
        return self.client.indices.exists(index=index)

