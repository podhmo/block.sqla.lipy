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
Env = namedtuple("Env", "handler query_methods action_factory")

class InvalidQueryMethod(Exception):
    pass
class HandleActionNotFound(Exception):
    pass
class PlaceHolderNameConflict(Exception):
    pass

class Name(object):
    def __init__(self, name):
        self.name = name

    def on_callback(self, action):
        return action(self)

class OnPlaceHolderActionFactory(object):
    def render(self, render_vals):
        def callback(placeholder):
            return render_vals[placeholder.name]
        return callback

    def collect(self, collected, history):
        def callback(placeholder):
            collected[placeholder.name].append(".".join(str(e) for e in history))
        return callback

class QueryTarget(object):
    def __init__(self, env, args):
        self.env = env
        self.args = args

    def __action__(self, context, callback=None):
        handle = self.env.handler.handle
        return [handle(e, context.get("history",[]), callback=callback) for e in self.args]

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

    def __action__(self, context, callback=None):
        history = context.get("history",[])
        result = self.env.handler.handle(self.args, history, callback=callback)
        return result

class ReverseHandler(object):
    def __init__(self, reverse_table):
        self.reverse_table = reverse_table

    def handle(self, e, history, callback=None): #todo: refactoring
        if hasattr(e, "on_callback"):
            return e.on_callback(callback)
        try:
            m = inspect(e)
            if hasattr(m, "key") and hasattr(m, "class_"): #User.id
                return ":{}".format(str(m))
            elif hasattr(m, "key") and hasattr(m, "value"): # User.id == 1 <- 
                return self.handle(m.value, history, callback=callback)
            elif hasattr(m, "name") and hasattr(m, "_annotations"): # -> User.id == 1
                return ":{}.{}".format(m._annotations["parententity"].class_.__name__, m.name)
            elif hasattr(m, "operator") and hasattr(m, "clauses"): # x && y
                op = self.reverse_table[m.operator]
                args = []
                for i, x in enumerate(m.clauses):
                    history.append(i+1)
                    args.append(self.handle(x, history, callback=callback))
                    history.pop()
                args.insert(0, op)
                return args
            elif hasattr(m, "mapper"): #User
                return ":{}".format(m.mapper.class_.__name__)
            elif hasattr(m, "left") and hasattr(m, "right"): #User.id == 1
                v = [self.reverse_table[m.operator]]
                history.append(1)
                v.append(self.handle(m.left, history, callback=callback))
                history.pop()
                history.append(2)
                v.append(self.handle(m.right, history, callback=callback))
                history.pop()
                return v
            elif hasattr(m, "modifier") and hasattr(m, "element"): #sa.desc(User.id)
                op = self.reverse_table[m.modifier]
                v = [op]
                history.append(1)
                v.append(self.handle(m.element, history, callback=callback))
                history.pop()
                return v
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
        callback = self.env.action_factory.render(kwargs)
        context = {"action": render_data}
        return render_data(self.data, context, callback=callback)

    def collect(self):
        context = {
            "action": collect_data,
            "history": [],
            "collected": defaultdict(list)
        }
        callback = self.env.action_factory.collect(context["collected"], context["history"]) #hmm.
        return collect_data(self.data, context, callback=callback)

    def __action__(self, context, callback):
        context["action"](self.data, context, callback=callback)


def render_data(data, context, callback):
    D = {}
    for k, v in data.items():
        if hasattr(v, "__action__"):
            D[k] = v.__action__(context, callback=callback)
        elif hasattr(v, "keys"):
            D[k] = render_data(v, context, callback)
        else:
            D[k] = v
    return D

def collect_data(data, context, callback):
    history = context["history"]
    for k, v in data.items():
        history.append(k)
        if hasattr(v, "__action__"):
            v.__action__(context, callback=callback)
        elif hasattr(v, "keys"):
            collect_data(v, context, callback)
        history.pop()
    return context["collected"]

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

def create_env(reverse_handler=None, query_methods=None, action_factory=None):
    reverse_handler = reverse_handler or create_reverse_handler()
    query_methods = query_methods or default_query_methods+default_lazy_options
    action_factory = action_factory or OnPlaceHolderActionFactory()
    return Env(
        action_factory=action_factory,
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

