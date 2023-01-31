from sqlalchemy import Integer, Column, String
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

PG_DSN = 'postgresql+asyncpg://user:1234@127.0.0.1:5431/asyncio'

engine = create_async_engine(PG_DSN)

Base = declarative_base()


class People(Base):

    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)


Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
