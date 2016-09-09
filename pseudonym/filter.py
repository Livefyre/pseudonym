from pseudonym.errors import InvalidConfigError


def str_to_slice(slice_str):
    return slice(*[int(k) if k else None for k in slice_str.split(':')])


class IndexFilter(object):
    def __init__(self, aliases=None, slice=None): # @ReservedAssignment
        self.aliases = aliases
        self.slice = str_to_slice(slice) if slice else None

    def filter(self, indexes):
        if self.aliases:
            indexes = [i for i in indexes if i.alias in self.aliases]

        is_sorted = False
        try:
            indexes = sorted(indexes, key=lambda i: i.routing, reverse=True)
            is_sorted = True
        except KeyError:
            pass

        if self.slice:
            if not is_sorted:
                raise InvalidConfigError("Indexes must use routing to be sliced.")
            indexes = indexes[self.slice]
        return indexes
