# -*- coding:utf-8 -*-
import logging
logger = logging.getLogger(__name__)

from sqlalchemy.inspection import inspect
from sqlalchemy.exc import NoInspectionAvailable
from block.sqla.lispy import (
    default_query_methods,
    default_lazy_options,
    default_args_method_table
)
from collections import namedtuple
Env = namedtuple("Env", "handler query_methods")

class InvalidQueryMethod(Exception):
    pass
class HandleActionNotFound(Exception):
    pass
class PlaceHolderNameConflict(Exception):
    pass

class Name(object):
    def __init__(self, name):
        self.name = name

    def __emit__(self, render_vals):
        return render_vals[self.name]

class QueryTarget(object):
    def __init__(self, env, args):
        self.env = env
        self.args = args

    def __render__(self, render_vals=None):
        handle = self.env.handler.handle
        return [handle(e, render_vals=render_vals) for e in self.args]

class QueryMethod(object):
    def __init__(self, name, data, args=None):
        self.name = name
        self.data = data
        self.args_method = args

    def update(self, args):
        if not self.name in self.data:
            new_data = self.data.copy()
        else:
            new_data = {"query": self.data.copy()}
        new_data[self.name] = args
        return ReverseQuery(args.env, new_data)


class ArgsMethod(object):
    def __init__(self, env, query_method, args=None):
        self.env = env
        self.query_method = query_method
        self.args = None

    def __call__(self, args):
        self.args = args #xxx
        return self.query_method.update(self)

    def __render__(self, render_vals=None):
        return self.env.handler.handle(self.args, render_vals=render_vals)


class ReverseHandler(object):
    def __init__(self, reverse_table):
        self.reverse_table = reverse_table

    def handle(self, e, render_vals=None): #todo: refactoring
        if hasattr(e, "__emit__"):
            return e.__emit__(render_vals)
        try:
            m = inspect(e)
            if hasattr(m, "key") and hasattr(m, "class_"): #User.id
                return ":{}".format(str(m))
            elif hasattr(m, "key") and hasattr(m, "value"): # User.id == 1 <- 
                return self.handle(m.value, render_vals=render_vals)
            elif hasattr(m, "name") and hasattr(m, "_annotations"): # -> User.id == 1
                return ":{}.{}".format(m._annotations["parententity"].class_.__name__, m.name)
            elif hasattr(m, "operator") and hasattr(m, "clauses"): # x && y
                op = self.reverse_table[m.operator]
                args = [self.handle(x, render_vals=render_vals) for x in m.clauses]
                args.insert(0, op)
                return args
            elif hasattr(m, "mapper"): #User
                return ":{}".format(m.mapper.class_.__name__)
            elif hasattr(m, "left") and hasattr(m, "right"): #User.id == 1
                return [self.reverse_table[m.operator],
                        self.handle(m.left, render_vals=render_vals),
                        self.handle(m.right, render_vals=render_vals)]
            elif hasattr(m, "modifier") and hasattr(m, "element"): #sa.desc(User.id)
                op = self.reverse_table[m.modifier]
                return [op, self.handle(m.element, render_vals=render_vals)]
            else:
                raise HandleActionNotFound(e)
        except NoInspectionAvailable:
            return e #1, 2, 3?

class ReverseQuery(object):
    def __init__(self, env, data=None):
        self.env = env
        self.data = data or {} #tail

    def __call__(self, *args):
        new_data = self.data.copy()
        new_data["query"] = QueryTarget(self.env, args)
        return self.__class__(self.env, new_data)

    def __getattr__(self, k):
        if k in self.env.query_methods:
            return ArgsMethod(self.env, QueryMethod(k, self.data.copy()))
        raise InvalidQueryMethod(k)

    def __render__(self, **kwargs):
        return render_data(self.data, kwargs)
    render = __render__

def render_data(data, render_vals):
    D = {}
    for k, v in data.items():
        if hasattr(v, "__render__"):
            D[k] = v.__render__(render_vals=render_vals)
        elif hasattr(v, "keys"):
            D[k] = render_data(v, render_vals)
        else:
            D[k] = v
    return D

class ReverseTable(object):
    default = {
        "notlike_op": "notlike",
        "like_op": "like",
        "desc_op": "desc",
        "asc_op": "asc"
    }
    def __init__(self, table, default=None):
        self.table = table
        self.default = default or self.__class__.default

    def __getitem__(self, k):
        try:
            return self.table[k]
        except KeyError:
            return self.default[k.__name__]


def create_reverse_handler(reverse_table=None):
    reverse_table = reverse_table or ReverseTable({v:k for k, v in default_args_method_table.items()})
    return ReverseHandler(reverse_table)

def create_env(reverse_handler=None, query_methods=None):
    reverse_handler = reverse_handler or create_reverse_handler()
    query_methods = query_methods or default_query_methods+default_lazy_options
    return Env(
        handler=reverse_handler, 
        query_methods=query_methods, 
    )

