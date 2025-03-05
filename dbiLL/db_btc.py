from sqlalchemy import Column, Integer, String, TIMESTAMP, Float
from sqlalchemy import create_engine, select
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime


"""
Модуль для создания, заполнения и использования базы данных котировок
Например из файлов собраных агрегатором https://github.com/Tucsky/aggr-server
Можно использовать sqlite, mySQL и другие (после небольшой модификации)
"""
Base = declarative_base()


class Exch(Base):
    """
    Класс для работы с базой бирж и пар (пока не используется)
    """
    __tablename__ = 'exch'

    id = Column(Integer, primary_key=True)
    name = Column(String(20))
    pair = Column(String(20))

    def __init__(self, name, pair):
        self.name = name
        self.pair = pair

    def __repr__(self):
        return f"Exch(id={self.id!r}, name={self.name!r}, pair={self.pair!r})"


class BTC(Base):
    """
    Класс для работы с базой котировок
    """
    __tablename__ = 'btc'

    time = Column(TIMESTAMP, nullable=False, primary_key=True)
    close = Column(Float)
    vol = Column(Float)
    dir = Column(Integer)
    liq = Column(Integer)

    def __init__(self, df):
        self.time = df.time
        self.close = df.close
        self.vol = df.vol
        self.dir = int(df.dir)
        self.liq = int(df.liq)

    def __repr__(self):
        return f"BTC(time={self.time!r}, close={self.close!r}, vol={self.vol!r}, dir={self.dir!r}, liq={self.liq!r})"


class Db():
    """
    Класс для работы с базами
    """
    def __init__(self, sql_base='sqlite', name_base='btc.db'):
        if sql_base == 'sqlite':
            connect_base = f'sqlite+pysqlite:///{name_base}'
        elif sql_base == 'mysql':
            connect_base = f'mysql://bitok:bitok@10.10.10.200:3307/{name_base}'
        else:
            connect_base = f'sqlite+pysqlite:///{name_base}'

        self.engine = create_engine(connect_base, echo=False, future=True)
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)()

    def open(self):
        # Session = sessionmaker(bind=self.engine)
        return self.session

    # def close(self, sess):
    #     sess.close()



if __name__ == '__main__':
    db = Db('sqlite', 'test.db')
    session = db.open()
    session.add(Exch("Binance", "btcusdt"))
    print(session.query(Exch).all())
    session.add(Exch("Bitmex", "xbtusd"))
    print(session.query(Exch).all())
    session.commit()
    print('Table name:', Exch.__table__)
    print(select(Exch))

    btc1 = BTC(datetime.fromtimestamp(1648735464660/1000, datetime.timezone.utc), 46350, 134, 1, None)
    session.add(btc1)
    print(session.query(BTC).all())    
    btc1 = BTC(datetime.fromtimestamp(1648735464660/1000, datetime.timezone.utc), 46350, 134, 1, None)
    session.add(btc1)
    print(session.query(BTC).all())    
    session.commit()
    print('Table name:', BTC.__table__)
    print(select(BTC))

