# -*- coding:utf-8 -*-
import unittest
import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.ext.declarative import declarative_base

class NameTests(unittest.TestCase):
    def setUp(self):
        from block.sqla.lispy.reverse import ReverseQuery
        from block.sqla.lispy.reverse import create_env

        Base = declarative_base()
        class User(Base):
            __tablename__ = "users"
            id = sa.Column(sa.Integer(), primary_key=True, nullable=False)
            name = sa.Column(sa.String(255), unique=True, nullable=False)

        self.User = User
        env = create_env()
        self.query_factory = ReverseQuery(env)

    def _useOne(self, *args, **kwargs):
        from block.sqla.lispy.reverse import Name
        return Name(*args, **kwargs)

    def test_it(self):
        q = self.query_factory(self.User)
        target = q.filter(self.User.id==self._useOne("user_id"))
        result = target.render(user_id=1)
        expected = q.filter(self.User.id==1).render()
        self.assertEqual(result, expected)

    def test_it2(self):
        q = self.query_factory(self.User)
        target = q.limit(self._useOne("limit"))
        result = target.render(limit=10)
        expected = q.limit(10).render()
        self.assertEqual(result, expected)

    def test_using_same_placeholder_name(self):
        q = self.query_factory(self.User)
        target = q.filter(self.User.id==self._useOne("v")).filter(self.User.name==self._useOne("v"))
        result = target.render(v=1)
        expected = q.filter(self.User.id==1).filter(self.User.name==1).render()
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
