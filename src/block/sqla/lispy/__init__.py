# -*- coding:utf-8 -*-
import sqlalchemy as sa
import operator as op

class InvalidElement(Exception):
    pass

# class Env(object):
#     def __init__(self, query_factory):
#         self.query_factory = query_factory

#     def eval(self, e):
#         self.e = e


class MapperHandler(object):
    def __init__(self, env, base):
        self.env = env
        self.base = base

    def match(self, e):
        return e.startswith(":")

    def handle(self, e):
        try:
            name = e[1:]
            return self.base._decl_class_registry[name]
        except KeyError:
            raise InvalidElement("{} is not found. .. Mapper".format(e))


class QueryProxy(object):
    def __init__(self, query):
        self.query = query
        self.lazy_options = []

    def __getattr__(self, k):
        return getattr(self.query, k)

    def __iter__(self):
        return iter(self.perform())

    def invoke_method(self, name, *args, **kwargs):
        method = getattr(self.query, name)
        self.query = method(*args, **kwargs)
        return self

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
    "quote": lambda *args: args,
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


class Parser(object):
    def __init__(self, query_factory,
                 macros=default_macros,
                 query_methods=["filter","order_by", "join", "options"], 
                 lazy_query_methods=["limit", "offset"], 
                 args_method_table=default_args_method_table
             ):
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
                    query = query.invoke_method(m, self.parse_args(data[m], query=query))
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
            if isinstance(data, (list, tuple)):
                return QueryProxy(self.query_factory(*data))
            else:
                return QueryProxy(self.query_factory(data))

    def parse_args(self, data, query=None):
        if isinstance(data, (tuple, list)):
            op = self.args_method_table[data[0]]
            args = [self.parse_args(e, query=query) for e in data[1:]]
            return op(*args)
        else:
            return data
