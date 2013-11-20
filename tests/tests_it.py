# -*- coding:utf-8 -*-
import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.ext.declarative import declarative_base
import unittest

class IntegrationTests(unittest.TestCase):
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

    def _getTarget(self):
        from block.sqla.lispy import create_parser
        return create_parser

    def _makeOne(self, *args, **kwargs):
        return self._getTarget()(*args, **kwargs)

    def test_it0(self):
        target = self._makeOne(self.Base, self.Session.query)
        data = {"query": {"query": [":User", ":Group"],
                          "filter": ["=", ":Group.id", 1]},
                "filter": ["=", ":User.group_id", ":Group.id"]
        }
        result = target(data).perform()
        q = self.Session.query(self.User, self.Group)
        expected = q.filter(self.Group.id==1).filter(self.User.group_id==self.Group.id)
        self.assertEqual(str(result), str(expected))

    def test_it(self):
        q = self.Session.query(self.Group, self.User)
        q = q.filter(self.Group.id==1).join(self.User, self.User.group_id==self.Group.id)
        expected = q.order_by(sa.desc(self.User.name)).limit(10)

        target = self._makeOne(self.Base, self.Session.query)
        data = {"limit": 10,
                "@cascade": [
                    {"query": [":Group", ":User"]},
                    {"filter": ["=", ":Group.id", 1]},
                    {"join": ["quote", ":User", ["==", ":User.group_id", ":Group.id"]]},
                    {"order_by": ["desc", ":User.name"]},
                ]}
        result = target(data).perform()
        self.assertEqual(str(result), str(expected))


if __name__ == '__main__':
    unittest.main()

