# -*- coding:utf-8 -*-
import sqlalchemy as sa
import operator as op

class InvalidElement(Exception):
    pass

class MapperHandler(object):
    def __init__(self, base):
        self.base = base

    def match(self, e):
        return hasattr(e, "startswith") and e.startswith(":")

    def handle(self, e):
        try:
            name_list = e[1:]
            nodes = name_list.split(".")
            name = nodes[0]
            attrs = nodes[1:]
            obj = self.base._decl_class_registry[name]
            for attr in attrs:
                try:
                    obj = getattr(obj, attr)
                except AttributeError:
                    raise InvalidElement("attribute {} is not found. .. Mapper.attribute".format(attr))
            return obj
        except KeyError:
            raise InvalidElement("{} is not found. .. Mapper".format(e))


class QueryProxy(object):
    def __init__(self, query, lazy_options=None):
        self.query = query
        self.lazy_options = lazy_options or []

    def __getattr__(self, k):
        attr = getattr(self.query, k)
        if callable(attr):
            def wrapped(*args, **kwargs):
                ## fixme:
                if isinstance(args[0], (list, tuple)):
                    args = args[0]
                new_query = attr(*args, **kwargs)
                return self.__class__(new_query, lazy_options=self.lazy_options[:])
            wrapped.__name__ = attr.__name__
            return wrapped
        else:
            return attr

    def __iter__(self):
        return iter(self.perform())

    def __str__(self):
        return str(self.perform())

    def perform(self):
        q = self.query
        for options in self.lazy_options:
            q = options(q)
        return q

def cascade(xs):
    """ {"@cascade": [{"query": U, "filter": ["=", "id", 1]}, {"filter": ["=", "name", "foo"]}]}
        => {"query": {"query": U, "filter": ["=", "id", 1]}, "filter": ["=", "name", "foo"]}
    """
    result = xs[0]
    for x in xs[1:]:
        nested = {}
        for k in x.keys():
            if k in result:
                nested[k] = x[k]
            else:
                result[k] = x[k]
        if nested:
            result = {"query": result}
            result.update(nested)
    return result


default_macros = {"cascade": cascade}
def quote(*args):
    return args

default_args_method_table = {
    "<": op.lt,
    "<=": op.le,
    ">": op.gt,
    ">=": op.ge,
    "=":  op.eq,
    "==":  op.eq,
    "!=": op.ne,
    "and": op.and_,
    "or": op.or_,
    "in": lambda x, y: x.in_(y),
    "quote": quote,
    "not": sa.not_,
    "like": lambda x, *args, **kwargs: getattr(x, "like")(*args, **kwargs),
    "desc": sa.desc,
    "asc": sa.asc
}

def list_from_one_or_many(e):
    if hasattr(e, "__iter__"):
        return list(e)
    else:
        return [e]

def merge_from_one_or_many(xs, ys):
    xs = list_from_one_or_many(xs)
    ys = list_from_one_or_many(ys)
    xs.extend(ys)
    return xs

def insert_bottom(query_dict, q):
    if not "query" in query_dict:
        query_dict["query"] = q
    else:
        sub = query_dict["query"]
        if not hasattr(sub, "keys"):
            query_dict["query"] = merge_from_one_or_many(sub, q)
        else:
            insert_bottom(sub, q)

class CompositeHandler(object):
    def __init__(self, handlers=None):
        self.handlers = handlers or []

    def match(self, e):
        return True

    def handle(self, e):
        for handler in self.handlers:
            if handler.match(e):
                return handler.handle(e)
        raise InvalidElement("handler not found: {}".format(e))

class IdentityHandler(object):
    def match(self, e):
        return True
    def handle(self, e):
        return e

class Parser(object):
    def __init__(self, query_factory,
                 handler, 
                 macros=default_macros,
                 query_methods=["filter","order_by", "join", "options"], 
                 lazy_query_methods=["limit", "offset"], 
                 args_method_table=default_args_method_table
             ):
        self.handler = handler
        self.query_factory = query_factory
        self.macros = macros
        self.query_methods = query_methods
        self.lazy_query_methods = lazy_query_methods
        self.args_method_table = args_method_table

    def __call__(self, data, query=None):
        data = self.parse_macro(data)
        return self.parse(data, query=query)

    def parse_macro(self, data):
        if hasattr(data, "keys"):
            ks = list(data.keys())
            for k in ks:
                v = self.parse_macro(data[k])
                if k.startswith("@"):
                    del data[k]
                    converted = self.macros[k[1:]](v)
                    if not "query" in data or not "query" in converted:
                        data.update(converted)
                    else:
                        insert_bottom(converted, data.pop("query"))
                        data.update(converted)
        elif isinstance(data, (tuple, list)):
            return [self.parse_macro(v) for v in data]
        return data

    def parse(self, data, query=None):
        if hasattr(data, "keys") and "query" in data:
            query = self.parse(data["query"], query=query)
            for m in self.query_methods:
                if m in data:
                    method = getattr(query, m)
                    query = method(self.parse_args(data[m], query=query))
            for m in self.lazy_query_methods:
                if m in data:
                    args = self.parse_args(data[m], query=query)
                    if not isinstance(args, (list, tuple)):
                        args = [args]
                    def lazy_action(q, name=m):
                        return getattr(q, name)(*args)
                    query.lazy_options.append(lazy_action)
            return query
        else:
            assert query is None
            handle = self.handler.handle
            if isinstance(data, (list, tuple)):
                return QueryProxy(self.query_factory(*(handle(e) for e in data)))
            else:
                return QueryProxy(self.query_factory(handle(data)))

    def parse_args(self, data, query=None):
        if isinstance(data, (tuple, list)):
            op = self.args_method_table[data[0]]
            args = [self.parse_args(e, query=query) for e in data[1:]]
            return op(*args)
        else:
            return self.handler.handle(data)

def create_handler(base):
    return CompositeHandler([MapperHandler(base), IdentityHandler()])

def create_parser(base, query_factory,
                  handler=None,
                  macros=default_macros,
                  query_methods=["filter","order_by", "join", "options"],
                  lazy_query_methods=["limit", "offset"],
                  args_method_table=default_args_method_table):
    handler = handler or create_handler(base)
    return Parser(query_factory,
                  handler,
                  macros=macros,
                  query_methods=query_methods,
                  lazy_query_methods=lazy_query_methods,
                  args_method_table=args_method_table)

def includeme(config):
    from zope.interface import Interface, provider
    class ILispyParserFactory(Interface):
        def __call__(*args, **kwargs):
            pass
    class ILispyParser(Interface):
        def __call__(data):
            pass

    def set_lispy_parser(config, *args, **kwargs):
        factory = config.registry.getUtility(ILispyParserFactory)
        parser = factory(*args, **kwargs)
        config.registry.registerUtility(provider(ILispyParser)(parser), ILispyParser)

    config.registry.registerUtility(provider(ILispyParserFactory)(create_handler), ILispyParserFactory)
    config.add_directive("set_lispy_parser", set_lispy_parser)
