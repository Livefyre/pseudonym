"""

Usage:
  pseudonym [options] index add <alias> <index> <routing>
  pseudonym [options] index remove <index>
  pseudonym [options] enforce
  pseudonym (-h --help)
  pseudonym --version

Options:
  -h --help        Show this screen.
  --version        Show version.
  --host=HOST      Elasticsearch hostname [default: localhost].
"""


from docopt import docopt
from elasticsearch.client import Elasticsearch
from pseudonym.manager import SchemaManager


def main():
    opts = docopt(__doc__)
    manager = SchemaManager(Elasticsearch(opts['--host']))
    if opts['remove']:
        manager.remove_index(opts['<index>'])
    if opts['add']:
        manager.add_index(opts['<alias>'], opts['<index>'], opts['<routing>'])
    if opts['enforce']:
        manager.enforce()
