from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, Text, Float, Boolean, JSON
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Affiliations(Base):
    __tablename__ = "affiliations"

    id = Column(Integer, primary_key=True)
    affiliate_id = Column(Integer)
    affiliate_string = Column(Text)
    embedding = Column(JSON)
    merged = Column(Boolean)
# connection
engine = create_engine("sqlite:///database.db")

# create metadata
Base.metadata.create_all(engine)

# create session
Session = sessionmaker(bind=engine)
session = Session()
