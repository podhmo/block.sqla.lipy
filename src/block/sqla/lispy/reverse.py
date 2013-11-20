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

class InvalidQueryMethod(Exception):
    pass
class HandleActionNotFound(Exception):
    pass


class QueryTarget(object):
    def __init__(self, env, args):
        self.env = env
        self.args = args

    def __render__(self):
        handle = self.env.handler.handle
        return [handle(e) for e in self.args]

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

    def __render__(self):
        return self.env.handler.handle(self.args)

from collections import namedtuple
Env = namedtuple("Env", "handler query_methods")

class ReverseHandler(object):
    def __init__(self, reverse_table):
        self.reverse_table = reverse_table

    def handle(self, e):
        try:
            m = inspect(e)
            if hasattr(m, "key") and hasattr(m, "class_"): #User.id
                return ":{}".format(str(m))
            elif hasattr(m, "key") and hasattr(m, "value"): # User.id == 1 <- 
                return m.value
            elif hasattr(m, "name") and hasattr(m, "_annotations"): # -> User.id == 1
                return ":{}.{}".format(m._annotations["parententity"].class_.__name__, m.name)
            elif hasattr(m, "operator") and hasattr(m, "clauses"): # x && y
                op = self.reverse_table[m.operator]
                args = [self.handle(x) for x in m.clauses]
                args.insert(0, op)
                return args
            elif hasattr(m, "mapper"): #User
                return ":{}".format(m.mapper.class_.__name__)
            elif hasattr(m, "left") and hasattr(m, "right"): #User.id == 1
                op = self.reverse_table[m.operator]
                return [op, self.handle(m.left), self.handle(m.right)]
            elif hasattr(m, "modifier") and hasattr(m, "element"): #sa.desc(User.id)
                op = self.reverse_table[m.modifier]
                return [op, self.handle(m.element)]
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

    def __render__(self):
        return render(self.data)
    render = __render__

def render(data):
    D = {}
    for k, v in data.items():
        if hasattr(v, "__render__"):
            D[k] = v.__render__()
        elif hasattr(v, "keys"):
            D[k] = render(v)
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

