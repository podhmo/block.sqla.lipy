block.sqla.lispy
----------------------------------------

setup

.. code:: python

    from block.sqla.lipsy import create_parser
    from myapp.models import Base
    from myapp.models import Session 

    parser = create_parser(Base,lambda : Session.query)

    ## model
    class Group(Base):
        __tablename__ = "groups"
        id = sa.Column(sa.Integer(), primary_key=True, nullable=False)
        name = sa.Column(sa.String(255), unique=True, nullable=False)

    class User(Base):
        __tablename__ = "users"
        id = sa.Column(sa.Integer(), primary_key=True, nullable=False)
        group_id = sa.Column(sa.Integer(), sa.ForeignKey("groups.id"))
        group = orm.relationship(Group, uselist=False, backref=("users"))
        name = sa.Column(sa.String(255), unique=True, nullable=False)

how to use
^^^^^^^^^^^^^^^^^^^^

simple

.. code:: python

    data = {"query": ":User",
            "filter": ["=", ":User.id", 1]}
    query = parser(data)

    ## equal
    ## query = Session.query(User).filter(User.id==1)

simple 2

.. code:: python 

    data = {"query": ":User", 
            "filter": ["and", ["not", ["like", ":User.name", "foo%"]], ["<" ":User.id" < 10]], 
            "order_by": ["desc", ":User.id"], 
            "limit": 10
    }
    query = parser(data)

    ## equal
    ## query = Session.query(User).filter(sa.and_(sa.not_(User.name.like("foo%"))),(User.id < 10))
    ## query = query.order_by(sa.desc(User.id)).limit(10) 


.. code:: python

    data = {"query": {"query": ":User", 
                     "filter": ["not", ["like", ":User.name", "foo%"]],
                     "order_by": ["desc", ":User.id"], 
                     "limit": 10},
            "filter": ["<" ":User.id" < 10]
    }
    query = parser(data)

    ## equal
    ## query = Session.query(User).filter(sa.not_(User.name.like("foo%"))).filter(User.id < 10))
    ## query = query.order_by(sa.desc(User.id)).limit(10) 


.. code:: python

    data = {
    "@cascade": [
            {"query": [":User.id", ":Group.name", ":User.name"]},
            {"filter": ["like", ":Group.name", "%Group%"]},
            {"filter": ["<=", ":Group.id", 1]}, 
            {"join": ["quote", ":User", ["==", ":User.group_id", ":Group.id"]]},
            {"order_by": ["desc", ":User.name"]},
            {"limit": 10},
        ]}
    query = parser(data)

    ## equal
    # q = self.Session.query(self.User.id, self.Group.name, self.User.name)
    # q = q.filter(self.Group.name.like("%Group%")).filter(self.Group.id <= 1)
    # q = q.join(self.Group, self.User.group_id==self.Group.id)
    # query = q.order_by(sa.desc(self.User.name)).limit(10)

