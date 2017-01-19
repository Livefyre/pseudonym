import time
import logging

from elasticsearch.helpers import scan, streaming_bulk, bulk, reindex

logger = logging.getLogger(__name__)

class Reindexer(object):

    def __init__(self, client):
        self.client = client

    def do_reindex(self, source_index, target_index, sleep_time):
        # Block updates to docs in source index
        self._set_read_only(source_index, True)
        self._set_scroll_sleep(sleep_time)
        errors = []

        try:
            # Using the python client reindex helper instead of native reindex api because it's not available until 2.3
            errors = reindex(self.client, source_index, target_index)
        except Exception as e:
            logger.exception("Reindex operation failed: %s" % e.message)
        finally:
            self._set_read_only(source_index, False)
            self.client.scroll = self.builtin_scroll
        return errors

    def _set_read_only(self, index, read_only):
        read_only_setting = {"index": {"blocks": {"write": read_only}}}
        self.client.indices.put_settings(index=index, body=read_only_setting)

    def _set_scroll_sleep(self, sleep_time):
        # Reindex helper uses scan/scroll and bulk update.  This scroll sleep helps prevent overloading the server
        def scroll_sleep(*args, **kwargs):
            time.sleep(float(sleep_time))  # seconds
            return self.builtin_scroll(*args, **kwargs)

        self.builtin_scroll = self.client.scroll
        self.client.scroll = scroll_sleep

