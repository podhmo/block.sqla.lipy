# -*- coding:utf-8 -*-
import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.ext.declarative import declarative_base
import unittest


class MapperHandlerTests(unittest.TestCase):
    def setUp(self):
        engine = sa.create_engine('sqlite://')
        Base = declarative_base(bind=engine)
        class User(Base):
            __tablename__ = "users"
            id = sa.Column(sa.Integer(), primary_key=True, nullable=False)
            name = sa.Column(sa.String(255), unique=True, nullable=False)

        self.Base = Base
        self.User = User

    def _getTarget(self):
        from block.sqla.lispy import MapperHandler
        return MapperHandler

    def _makeOne(self, *args, **kwargs):
        return self._getTarget()(*args, **kwargs)

    def test_match(self):
        target = self._makeOne(None,self.Base)
        self.assertTrue(target.match(":Gruop"))

    def test_match__fail(self):
        target = self._makeOne(None,self.Base)
        self.assertFalse(target.match("<"))

    def test_handle(self):
        target = self._makeOne(None,self.Base)
        result = target.handle(":User")
        self.assertEqual(result, self.User)

    def test_handle_fail(self):
        from block.sqla.lispy import InvalidElement
        target = self._makeOne(None,self.Base)
        with self.assertRaises(InvalidElement):
            target.handle(":Grou")


class CascadeTests(unittest.TestCase):
    def _callFUT(self, data):
        from block.sqla.lispy import cascade
        return cascade(data)

    def test_it(self):
        data = [{"query": "U", "filter": ["=", "id", 1]}, {"filter": ["=", "name", "foo"]}]
        result = self._callFUT(data)
        expected = {"query": {"query": "U", "filter": ["=", "id", 1]},
                    "filter": ["=", "name", "foo"]}
        self.assertEqual(result, expected)

    def test_it2(self):
        data = [{"query": "U"}, {"filter": ["=", "id", 1]}, {"filter": ["=", "name", "foo"]}]
        result = self._callFUT(data)
        expected = {"query": {"query": "U", "filter": ["=", "id", 1]},
                    "filter": ["=", "name", "foo"]}
        self.assertEqual(result, expected)

    def test_it3(self):
        data = [{"query": "U"}, {"order_by": ["asc", "id"]}, {"filter": ["=", "id", 1]}, {"filter": ["=", "name", "foo"]}]
        result = self._callFUT(data)
        expected = {'query': {'query': 'U', 
                              'order_by': ['asc', 'id'], 
                              'filter': ['=', 'id', 1]}, 
                    'filter': ['=', 'name', 'foo']}
        self.assertEqual(result, expected)

class ApplicableTests(unittest.TestCase):
    def setUp(self):
        Base = declarative_base()
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

        self.Base = Base
        self.User = User
        self.Group = Group
        self.Session = orm.sessionmaker()()

    def _callFUT(self, *args, **kwargs):
        from block.sqla.lispy import Parser
        return Parser(self.Session.query)(*args, **kwargs)

    def assertQuery(self, q1, q2):
        self.assertEqual(str(q1), str(q2))

    def test_0(self):
        data = {"query": self.User,
                "filter": ["=", self.User.id, 1]}
        result = self._callFUT(data).perfume()
        expected = self.Session.query(self.User).filter(self.User.id==1)
        self.assertQuery(result, expected)

    def test_1(self):
        data = {"query": self.User,
                "filter": ["!=", self.User.id, 1]}
        result = self._callFUT(data).perfume()
        expected = self.Session.query(self.User).filter(self.User.id!=1)
        self.assertQuery(result, expected)

    def test_2(self):
        data = {"query": self.User,
                "filter": ["in", self.User.id, ["quote", 1, 2, 3]]}
        result = self._callFUT(data).perfume()
        expected = self.Session.query(self.User).filter(self.User.id.in_([1, 2, 3]))
        self.assertQuery(result, expected)

    def test_3(self):
        data = {"query": self.User,
                "filter": ["not", ["in", self.User.id, ["quote", 1, 2, 3]]]}
        result = self._callFUT(data).perfume()
        expected = self.Session.query(self.User).filter(sa.not_(self.User.id.in_([1, 2, 3])))
        self.assertQuery(result, expected)

    def test_4(self):
        data = {"query": self.User, 
                "filter": ["and",  ["or", ["=", self.User.name, "foo"], ["=", self.User.name, "bar"]], 
                            ["<", self.User.id, 10]]}

        result = self._callFUT(data).perfume()
        q =  self.Session.query(self.User)
        expected = q.filter(sa.and_(sa.or_(self.User.name=="foo", self.User.name=="bar"), self.User.id<10))
        self.assertQuery(result, expected)

    def test_5(self):
        data = {"query": {"query": self.User,
                          "filter": ["=", self.User.id, 1]},
                "filter": ["like", self.User.name, "%foo%"]
        }
        result = self._callFUT(data).perfume()
        q = self.Session.query(self.User)
        expected = q.filter(self.User.id==1).filter(self.User.name.like("%foo%"))
        self.assertQuery(result, expected)

    def test_6__option1(self):
        data = {"query": self.User, 
                "filter": ["=", self.User.id, 1], 
                "order_by": ["desc", self.User.id], 
                "limit": ["quote", 10]
        }
        result = self._callFUT(data).perfume()
        q = self.Session.query(self.User).filter(self.User.id==1)
        expected = q.order_by(sa.desc(self.User.id)).limit(10)
        self.assertQuery(result, expected)

    def test_7__option2(self):
        data = {"query": self.User, 
                "filter": ["=", self.User.id, 1], 
                "order_by": ["desc", self.User.id], 
                "limit": 10
        }
        result = self._callFUT(data).perfume()
        q = self.Session.query(self.User).filter(self.User.id==1)
        expected = q.order_by(sa.desc(self.User.id)).limit(10)
        self.assertQuery(result, expected)

    def test_8__join_by_filter(self):
        data = {"query": {"query": [self.User, self.Group],
                          "filter": ["=", self.Group.id, 1]},
                "filter": ["=", self.User.group_id, self.Group.id]
        }
        result = self._callFUT(data).perfume()
        q = self.Session.query(self.User, self.Group)
        expected = q.filter(self.Group.id==1).filter(self.User.group_id==self.Group.id)
        self.assertQuery(result, expected)

    def test_9__join_by_join(self):
        data = {"query": [self.Group, self.User],
                "filter": ["=", self.Group.id, 1], 
                "join": self.User
        }
        result = self._callFUT(data).perfume()
        q = self.Session.query(self.Group, self.User)
        expected = q.filter(self.Group.id==1).join(self.User)
        self.assertQuery(result, expected)

    def test_10__join_by_join__with_arguments(self):
        data = {"query": [self.Group, self.User],
                "filter": ["=", self.Group.id, 1],
                "join": ["quote", self.User, ["=", self.User.group_id, self.Group.id]]
        }
        result = self._callFUT(data).perfume()
        q = self.Session.query(self.Group, self.User)
        expected = q.filter(self.Group.id==1).join(self.User, self.User.group_id==self.Group.id)
        self.assertQuery(result, expected)

    def test_11__complex_cascade(self):
        data = {"@cascade": [
            {"query": self.User}, 
            {"filter": ["like", self.Group.name, "%foo%"]}, 
            {"filter": ["=", self.User.group_id, self.Group.id]}, 
            {"order_by": ["desc", self.User.name]}, 
            {"limit": 10}
        ]}
        result = self._callFUT(data).perfume()
        q = self.Session.query(self.User)
        q = q.filter(self.Group.name.like("%foo%"))
        q = q.filter(self.User.group_id==self.Group.id)
        expected = q.order_by(sa.desc(self.User.name)).limit(10)
        self.assertQuery(result, expected)


if __name__ == '__main__':
    unittest.main()
