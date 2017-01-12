import time
import logging

from elasticsearch.helpers import scan, streaming_bulk, bulk

logger = logging.getLogger(__name__)

class Reindexer(object):

    def __init__(self, client):
        self.client = client

    def do_reindex(self, source_index, target_index, sleep_time):
        # Block updates to docs in source index
        # self._set_read_only(source_index, True)
        self._set_scroll_sleep(sleep_time)

        try:
            # Using the python client reindex helper instead of native reindex api because it's not available until 2.3
            reindex(self.client, source_index, target_index)
        except Exception as e:
            logger.exception("Reindex operation failed: %s" % e.message)
        finally:
            self._set_read_only(source_index, False)
            self.client.scroll = self.builtin_scroll

    def _set_read_only(self, index, read_only):
        read_only_setting = {"index": {"blocks": {"read_only": read_only}}}
        self.client.indices.put_settings(index=index, body=read_only_setting)

    def _set_scroll_sleep(self, sleep_time):
        # Reindex helper uses scan/scroll and bulk update.  This scroll sleep helps prevent overloading the server
        def scroll_sleep(*args, **kwargs):
            time.sleep(sleep_time)  # seconds
            return self.builtin_scroll(*args, **kwargs)

        self.builtin_scroll = self.client.scroll
        self.client.scroll = scroll_sleep

    # def reindex_stop(self, source_index):
    #     # clear scroll won't work because client hides scroll_id from us
    #     self.client.clear_scroll(scroll_id=scroll_id)


# patch of bulk method to remove reindexed docs from source index once they've been moved
def bulk_with_delete(client, actions, source_index, stats_only=False, **kwargs):
    delete = []
    for ok, doc in streaming_bulk(client, actions, **kwargs):
        # Delete reindexed docs from source index so we can easily pause/resume reindexing
        try:
            if ok:
                delete.append({
                    '_op_type': 'delete',
                    '_index': source_index,
                    '_type': doc['index']['_type'],
                    '_id': doc['index']['_id']
                })
            if len(delete) >= kwargs['chunk_size']:
                bulk(client, delete)
                delete = []
        except Exception as e:
            logger.exception("Could not remove doc from source index after reindexing: %s" % doc)

    # Remove last (or only) batch of docs
    if delete:
        bulk(client, delete)

# patch of elasticsearch.helpers.reindex so we can remove reindexed docs from source index inline
def reindex(client, source_index, target_index, query=None, target_client=None,
            chunk_size=500, scroll='5m', scan_kwargs={}, bulk_kwargs={}):
    """
    Reindex all documents from one index that satisfy a given query
    to another, potentially (if `target_client` is specified) on a different cluster.
    If you don't specify the query you will reindex all the documents.

    Since ``2.3`` a :meth:`~elasticsearch.Elasticsearch.reindex` api is
    available as part of elasticsearch itself. It is recommended to use the api
    instead of this helper wherever possible. The helper is here mostly for
    backwards compatibility and for situations where more flexibility is
    needed.

    .. note::

        This helper doesn't transfer mappings, just the data.

    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use (for
        read if `target_client` is specified as well)
    :arg source_index: index (or list of indices) to read documents from
    :arg target_index: name of the index in the target cluster to populate
    :arg query: body for the :meth:`~elasticsearch.Elasticsearch.search` api
    :arg target_client: optional, is specified will be used for writing (thus
        enabling reindex between clusters)
    :arg chunk_size: number of docs in one chunk sent to es (default: 500)
    :arg scroll: Specify how long a consistent view of the index should be
        maintained for scrolled search
    :arg scan_kwargs: additional kwargs to be passed to
        :func:`~elasticsearch.helpers.scan`
    :arg bulk_kwargs: additional kwargs to be passed to
        :func:`~elasticsearch.helpers.bulk`
    """
    target_client = client if target_client is None else target_client

    docs = scan(client,
                query=query,
                index=source_index,
                scroll=scroll,
                **scan_kwargs
                )

    def _change_doc_index(hits, index):
        for h in hits:
            h['_index'] = index
            if 'fields' in h:
                h.update(h.pop('fields'))
            yield h

    kwargs = {
        'stats_only': True,
    }

    kwargs.update(bulk_kwargs)
    bulk_with_delete(target_client, _change_doc_index(docs, target_index),
                source_index, chunk_size=chunk_size, **kwargs)

