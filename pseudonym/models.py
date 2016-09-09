import collections


# http://stackoverflow.com/questions/11351032/named-tuple-and-optional-keyword-arguments
def namedtuple_with_defaults(typename, field_names, default_values=tuple()):
    T = collections.namedtuple(typename, field_names)
    T.__new__.__defaults__ = (None,) * len(T._fields)
    if isinstance(default_values, collections.Mapping):
        prototype = T(**default_values)
    else:
        prototype = T(*default_values)
    T.__new__.__defaults__ = tuple(prototype)
    return T

Index = namedtuple_with_defaults('Index', ('name', 'alias', 'routing', 'mappings', 'settings'))
