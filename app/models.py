from sqlalchemy import Column, Integer, String, Date, Float
from .db import Base

class User(Base):
    __tablename__ = "users"
    user_id = Column(Integer, primary_key=True, index=True)
    salary_day = Column(Integer, default=25)     # оклад
    advance_day = Column(Integer, default=10)    # аванс
    min_contrib = Column(Integer, default=40000)
    max_contrib = Column(Integer, default=50000)
    risk = Column(String, default="balanced")

class Contribution(Base):
    __tablename__ = "contribs"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, index=True)
    date = Column(Date)
    amount = Column(Float)
    source = Column(String, default="manual")  # "salary" | "advance" | "manual" | "adjustment"
