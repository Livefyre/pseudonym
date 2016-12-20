import time
import logging

from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import reindex, bulk, scan

logger = logging.getLogger(__name__)

class Reindexer(object):

    def __init__(self, client):
        self.client = client

    def reindex(self, source_index, target_index):
        return self._reindex_copy(source_index, target_index)

    def catchup(self, source_index, reindex_start_time):
        # if timestamp was updated since the reindex then copy those updated docs to new index
        query = {"query": { "range" : { "_timestamp" : { "gte": reindex_start_time}}}}
        target_index = '%s_new' % source_index
        return self._reindex_copy(source_index, target_index, query=query)

    def cutover(self, source_index, reindex_start_time):
        # add new index to cluster/aliases
        # remove old index from cluster/aliases
        # do one last catchup call to get any remaining updates
        #TODO delete old index completely?
        target_index = '%s_new' % source_index
        meta, schema = self.get_current_schema(True)
        for alias in schema['aliases']:
            if source_index in alias['indexes']:
                self.add_index(alias['name'], target_index)
                self.remove_index(source_index)
        self.catchup(source_index, reindex_start_time)

    def update_timestamps(self, index_name, docs):
        # bulk update _timestamp to now
        start_time = int(time.time())
        actions = []
        for doc in docs:
            actions.append({
                '_op_type': 'update',
                '_index': index_name,
                '_type': doc['_type'],
                '_id': doc['_id'],
                'doc': {'_timestamp': start_time}
            })
        try:
            # This will send chunks of 500 updates to ES at a time, by default
            bulk(self.client, actions)
        except Exception as e:
            logger.exception("Timestamp update failed: %s" % e.message)

        return start_time

    def _scan_index(self, index_name, query=None):
        try:
            return list(scan(self.client, index=index_name, query=query))
        except NotFoundError:
            logger.exception("Index does not exist: %s" % index_name)

    def _reindex_copy(self, source_index, target_index, query=None):
        # Save current timestamp to docs in old index, then do reindex
        docs = self._scan_index(source_index, query)

        if not docs:
            logger.exception("No docs found to reindex")
            return

        last_updated = self.update_timestamps(source_index, docs)
        try:
            result = reindex(self.client, source_index, target_index, query=query)
        except Exception as e:
            logger.exception("Reindex operation failed: %s" % e.message)
            return

        result['last_updated'] = last_updated
        '''{
          "took" : 639,
          "updated": 112,
          "batches": 130,
          "version_conflicts": 0,
          "failures" : [ ],
          "created": 12344
        } '''
        return result
