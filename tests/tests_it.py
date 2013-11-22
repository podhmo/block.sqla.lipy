# -*- coding:utf-8 -*-
import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.ext.declarative import declarative_base
import unittest

class IntegrationTests(unittest.TestCase):
    def tearDown(self):
        self.Base.metadata.drop_all()

    def setUp(self):
        engine = sa.create_engine("sqlite://")
        Base = declarative_base(bind=engine)
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
        self.Session = orm.sessionmaker(bind=engine)()
        self.Base.metadata.create_all()

    def setupFixture(self):
        s = self.Session()
        group1 = self.Group(name="Group1")
        group2 = self.Group(name="Group2")
        s.add(group1)
        s.add(group2)
        s.add(self.User(name="foo", group=group1))
        s.add(self.User(name="boo", group=group1))
        s.add(self.User(name="bar", group=group2))
        s.commit()

    def _getTarget(self):
        from block.sqla.lispy import create_parser
        return create_parser

    def _makeOne(self, *args, **kwargs):
        return self._getTarget()(*args, **kwargs)

    def test_it0(self):
        target = self._makeOne(self.Base, self.Session.query)
        data = {"query": {"query": [":User", ":Group"],
                          "filter": ["=", ":Group.name", "Group1"]},
                "filter": ["=", ":User.group_id", ":Group.id"]
        }
        result = target(data)

        q = self.Session.query(self.User, self.Group)
        expected = q.filter(self.Group.name=="Group1").filter(self.User.group_id==self.Group.id)

        self.assertEqual(str(result), str(expected))
        self.assertEqual(list(result), list(expected))

    def test_it(self):
        target = self._makeOne(self.Base, self.Session.query)
        data = {"limit": 10,
                "@cascade": [
                    {"query": [":User.id", ":Group.name", ":User.name"]},
                    {"filter": ["like", ":Group.name", "%Group%"]},
                    {"filter": ["<=", ":Group.id", 1]}, 
                    {"join": ["quote", ":Group", ["=", ":User.group_id", ":Group.id"]]},
                    {"order_by": ["desc", ":User.name"]},
                ]}
        result = target(data)

        q = self.Session.query(self.User.id, self.Group.name, self.User.name)
        q = q.filter(self.Group.name.like("%Group%")).filter(self.Group.id <= 1)
        q = q.join(self.Group, self.User.group_id==self.Group.id)
        expected = q.order_by(sa.desc(self.User.name)).limit(10)


        self.assertEqual(str(result), str(expected))
        self.assertEqual(list(result), list(expected))

if __name__ == '__main__':
    unittest.main()

