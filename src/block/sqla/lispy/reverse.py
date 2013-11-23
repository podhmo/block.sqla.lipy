# -*- coding:utf-8 -*-
import logging
logger = logging.getLogger(__name__)

from sqlalchemy.inspection import inspect
from sqlalchemy.exc import NoInspectionAvailable
from block.sqla.lispy import (
    default_query_methods,
    default_lazy_options,
    default_args_method_table,
)
from collections import namedtuple, defaultdict
Env = namedtuple("Env", "handler query_methods context_factory")

class InvalidQueryMethod(Exception):
    pass
class HandleActionNotFound(Exception):
    pass
class MatchedClauesNotFound(Exception):
    pass

class Name(object):
    def __init__(self, name):
        self.name = name

    def on_callback(self, action):
        return action(self)

def scan_data(context, data):
    for k, v in data.items():
        if hasattr(v, "__action__"):
            context.climb_down(v.__action__, k)
        elif hasattr(v, "keys"):
            context.climb_down(scan_data, k, v)
        else:
            raise MatchedClauesNotFound(repr(k, v))
    return context.finish()

class RenderContext(object):
    def __init__(self, render_vals, action=scan_data):
        self.render_vals = render_vals
        self.stack = [{}]
        self.action = action

    @property
    def result(self):
        return self.stack[-1]
    @result.setter
    def result(self, v):
        self.stack[-1] = v

    def emit(self, placeholder):
        return self.render_vals[placeholder.name]

    def subscan(self, handler, k, data):
        return handler.handle(self, data)

    def climb_down(self, fn, k, *args):
        self.stack.append({})
        result = fn(self, *args)
        self.stack.pop()
        self.result[k] = result

    def finish(self):
        return self.result

class CollectContext(object):
    def __init__(self, history, collected, action=scan_data):
        self.history = history
        self.collected = collected
        self.action = action

    def emit(self, placeholder):
        self.collected[placeholder.name].append(".".join(str(e) for e in self.history))

    def subscan(self, handler, k, data):
        return self.climb_down(handler.handle, k, data)

    def climb_down(self, fn, k, *args):
        self.history.append(k)
        v = fn(self, *args)
        self.history.pop()
        return v

    def finish(self):
        return self.collected

class DefaultContextFactory(object):
    render = RenderContext
    collect = CollectContext

class ReverseHandler(object):
    def __init__(self, reverse_table):
        self.reverse_table = reverse_table

    def scan(self, context, k, e):
        return context.subscan(self, k, e)

    def handle(self, context, e): #todo: refactoring
        if hasattr(e, "on_callback"):
            return e.on_callback(context.emit) #hmm.
        try:
            m = inspect(e)
            if hasattr(m, "key") and hasattr(m, "class_"): #User.id
                return ":{}".format(str(m))
            elif hasattr(m, "key") and hasattr(m, "value"): # User.id == 1 <- 
                return self.handle(context, m.value)
            elif hasattr(m, "name") and hasattr(m, "_annotations"): # -> User.id == 1
                return ":{}.{}".format(m._annotations["parententity"].class_.__name__, m.name)
            elif hasattr(m, "operator") and hasattr(m, "clauses"): # x & y,  x | y
                op = self.reverse_table[m.operator]
                args = [self.scan(context, str(i), x) for i, x in enumerate(m.clauses)]
                args.insert(0, op)
                return args
            elif hasattr(m, "mapper"): #User
                return ":{}".format(m.mapper.class_.__name__)
            elif hasattr(m, "left") and hasattr(m, "right"): #User.id == 1
                return [self.reverse_table[m.operator],
                        self.scan(context, "1", m.left),
                        self.scan(context, "2", m.right)
                    ]
            elif hasattr(m, "modifier") and hasattr(m, "element"): #sa.desc(User.id)
                return [self.reverse_table[m.modifier], self.scan(context, "1", m.element)]
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

    def render(self, **kwargs):
        generate_context = self.env.context_factory.render
        context = generate_context(kwargs, action=scan_data)
        return scan_data(context, self.data)

    def collect(self):
        generate_context = self.env.context_factory.collect
        context = generate_context(history=[], collected=defaultdict(list), action=scan_data)
        return scan_data(context, self.data)

    def __action__(self, context):
        return context.action(context, self.data)

class QueryTarget(object):
    def __init__(self, env, args):
        self.env = env
        self.args = args

    def __action__(self, context):
        handle = self.env.handler.handle
        targets = [handle(context, e) for e in self.args]
        return targets

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

    def __action__(self, context):
        result = self.env.handler.handle(context, self.args)
        return result

class ReverseTable(object):
    default = {
        "notlike_op": "notlike",
        "like_op": "like",
        "desc_op": "desc",
        "asc_op": "asc",
        "in_op": "in",
        "notin_op": "not_in",#xxx:
        "comma_op": "quote",#xxx:
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

def create_env(reverse_handler=None, query_methods=None, context_factory=None):
    reverse_handler = reverse_handler or create_reverse_handler()
    query_methods = query_methods or default_query_methods+default_lazy_options
    context_factory = context_factory or DefaultContextFactory
    return Env(
        context_factory=context_factory,
        handler=reverse_handler, 
        query_methods=query_methods, 
    )


## hmm.
def replace(access_dict, data, **kwargs):
    for access_k, v in kwargs.items():
        for name in access_dict[access_k]:
            target = data
            nodes = name.split(".")

            for k in nodes[:-1]:
                target = target[k]
            if isinstance(target, (list, tuple)):
                target[int(nodes[-1])] = v
            else:
                target[nodes[-1]] = v
    return data

