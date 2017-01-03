import time
import logging

from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import reindex, bulk

logger = logging.getLogger(__name__)

class Reindexer(object):

    def __init__(self, client):
        self.client = client
        self.builtin_scroll = self.client.scroll
        self.client.scroll = self.scroll_sleep
        self.read_only_setting = {"index": {"blocks": {"read_only": True}}}

        # Reindex helper uses scan/scroll and bulk update.  This scroll sleep helps prevent killing the server
        def scroll_sleep(*args, **kwargs):
            time.sleep(30) # seconds
            self.builtin_scroll(*args, **kwargs)

    def reindex(self, source_index, target_index):
        # Disable updates to docs in source index
        self._set_read_only(source_index)

        try:
            # Using the reindex helper instead of native reindex api because it's backward compatible and more flexible
            result = reindex(self.client, source_index, target_index)
            if not result:
                logger.exception("No docs found for reindexing")
                return
        except Exception as e:
            logger.exception("Reindex operation failed: %s" % e.message)
            return

        # Delete reindexed docs from source index so we can easily pause/resume reindexing
        try:
            actions = []
            for doc in result.hits.hits:
                actions.append({
                    '_op_type': 'delete',
                    '_index': source_index,
                    '_type': doc['_type'],
                    '_id': doc['_id']
                })
            # This will send chunks of 500 deletions to ES at a time, by default
            bulk(self.client, actions)
        except Exception as e:
            logger.exception("Unable to delete docs from old index after reindexing them: %s" % e.message)

    def _set_read_only(self, index):
        self.client.indices.put_settings(index=index, body=self.read_only_setting)

    def cutover(self, source_index, schema):
        # add new index to cluster/aliases
        # remove old index from cluster/aliases
        #TODO delete old index completely?  Should be 0 docs left

        target_index = '%s_new' % source_index
        for alias in schema['aliases']:
            if source_index in alias['indexes']:
                self.add_index(alias['name'], target_index)
                self.remove_index(source_index)
