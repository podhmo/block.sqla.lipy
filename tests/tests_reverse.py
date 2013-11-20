# -*- coding:utf-8 -*-

import unittest
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base


class ReverseHandlerTests(unittest.TestCase):
    def _makeOne(self, *args, **kwargs):
        from block.sqla.lispy.reverse import create_reverse_handler
        return create_reverse_handler()

    def setUp(self):
        Base = declarative_base()
        class User(Base):
            __tablename__ = "users"
            id = sa.Column(sa.Integer(), primary_key=True, nullable=False)
            name = sa.Column(sa.String(255), unique=True, nullable=False)

        self.Base = Base
        self.User = User

    def test_model_class(self):
        target = self._makeOne()
        result = target.handle(self.User)
        self.assertEqual(result, ":User")

    def test_model_attribute(self):
        target = self._makeOne()
        result = target.handle(self.User.id)
        self.assertEqual(result, ":User.id")

        result = target.handle(self.User.name)
        self.assertEqual(result, ":User.name")

    def test_bop(self):
        target = self._makeOne()
        result = target.handle(self.User.id==1)
        self.assertEqual(result, ['=', ':User.id', 1])
        result = target.handle(1==self.User.id)
        self.assertEqual(result, ['=', ':User.id', 1])

    def test_bop2(self):
        target = self._makeOne()
        result = target.handle(sa.not_(self.User.name=="foo"))
        self.assertEqual(result, ['!=', ':User.name', "foo"])

    def test_bop3(self):
        target = self._makeOne()
        result = target.handle(sa.not_(self.User.name.like("%foo%")))
        self.assertEqual(result, ['notlike', ':User.name', "%foo%"])
        result = target.handle((self.User.name.like("%foo%")))
        self.assertEqual(result, ['like', ':User.name', "%foo%"])

    def test_bop4(self):
        target = self._makeOne()
        result = target.handle(sa.or_(self.User.name.like("%foo%"), self.User.name.like("%bar")))
        self.assertEqual(result, ['or', ['like', ':User.name', '%foo%'], ['like', ':User.name', '%bar']])
        result = target.handle(sa.and_(self.User.name.like("%foo%"), sa.not_(self.User.id != 1)))
        self.assertEqual(result, ['and', ['like', ':User.name', '%foo%'], ['=', ':User.id', 1]])



class ReverseQueryRenderingTests(unittest.TestCase):
    def setUp(self):
        from block.sqla.lispy.reverse import create_env
        Base = declarative_base()
        class Group(Base):
            __tablename__ = "groups"
            id = sa.Column(sa.Integer(), primary_key=True, nullable=False)
            name = sa.Column(sa.String(255), unique=True, nullable=False)

        class User(Base):
            __tablename__ = "users"
            id = sa.Column(sa.Integer(), primary_key=True, nullable=False)
            name = sa.Column(sa.String(255), unique=True, nullable=False)


        self.env = create_env()
        self.Group = Group
        self.User = User

    def _makeOne(self, *args, **kwargs):
        from block.sqla.lispy.reverse import ReverseQuery
        return ReverseQuery(*args, **kwargs)

    def test_1(self):
        target = self._makeOne(self.env)
        result = target(self.Group).render()
        self.assertEqual(result, 
                         {'query': [':Group']})

    def test_2(self):
        target = self._makeOne(self.env)
        result = target(self.Group).filter(self.Group.id==1).render()
        self.assertEqual(result, 
                         {'filter': ['=', ':Group.id', 1], 'query': [':Group']})
    def test_3(self):
        target = self._makeOne(self.env)
        result = target(self.Group).filter(self.Group.id==1).filter(self.Group.name=="foo").render()
        self.assertEqual(result, 
                         {'filter': ['=', ':Group.name', 'foo'],
                          'query': {'filter': ['=', ':Group.id', 1],
                                    'query': [':Group']}})

    def test_4(self):
        target = self._makeOne(self.env)
        q = target(self.Group, self.User).filter(sa.or_(self.Group.name.like("%foo%"), self.Group.name.like("%bar")))
        result = q.join(self.User).order_by(sa.desc(self.User.id)).limit(10).render()
        self.assertEqual(result, 
                         {'join': ':User', 
                          'filter': ['or', ['like', ':Group.name', '%foo%'], ['like', ':Group.name', '%bar']], 
                          'query': [':Group', ':User'],
                          'limit': 10,
                          'order_by': ['desc', ':User.id']})

if __name__ == '__main__':
    unittest.main()

