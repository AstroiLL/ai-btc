import os
import pandas as pd
from dbiLL.db_btc import Db, BTC
from datetime import datetime, timedelta
import logging
"""
Из файлов собранных агрегатором https://github.com/Tucsky/aggr-server
(Агрегатор должен работать непрерывно столько времени, сколько данных вам надо собрать)
Вытаскиваются все максимумы объемов >= moreBTC за каждый час
И складываются в базу SQL (например sqlite или mySQL)
Начало сбора максимумов start_date до текущего момента now_date
В последствии эту базу можно использовать для визуализации на графиках и для использования в ML
Для работы с SQL через SQLAlchemy используется модуль dbiLL.db_btc
Запускать программу можно много раз, она добавляет только отсутствующие значения
"""

# Настройка логирования
logging.basicConfig(filename='aggr_max_vol.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Константы
MORE_BTC_THRESHOLD = 10
MAXIMUM_VOLUME_THRESHOLD = 500
DATA_PATH = '/home/astroill/BTC/aggr-server/data'
START_DATE = '2025-03-04'


def main():
    """
    Обрабатывает данные из файлов, собранных aggr-server, и сохраняет
    информацию о всплесках объема BTC в базу данных.
    """
    now_date = datetime.now().strftime("%Y-%m-%d")
    print('Start:', START_DATE)
    print("Now:", now_date)
    # База данных для занесения всплесков
    db = Db('sqlite', './DB/btc.db')
    with db.open() as session:
        for dirs, folder, files in os.walk(DATA_PATH):
            for file in files:
                fname, ext = os.path.splitext(file)
                date = fname[:10]
                if ext == '.gz' and date >= START_DATE:
                    dir1, folder2 = os.path.split(dirs)
                    _, folder1 = os.path.split(dir1)
                    fullname = os.path.join(dirs, file)
                    try:
                        df = pd.read_csv(
                            fullname, sep=' ', compression='gzip', names=['time', 'close', 'vol', 'dir', 'liq'], on_bad_lines='skip',
                            dtype={'time': 'Int64', 'close': 'float64', 'vol': 'float64', 'dir': 'Int32', 'liq': 'Int32'}
                        )

                    except FileNotFoundError:
                        logging.error(f"File not found: {fullname}")
                        continue
                    except Exception as e:
                        logging.exception(f"Error processing file {fullname}: {e}")
                        continue
                    df.fillna(0, inplace=True)
                    df = df[['time', 'close', 'vol', 'dir', 'liq']]
                    df['time'] = pd.to_datetime(df['time'], unit='ms')
                    # Всплеск берется если объем >= MORE_BTC_THRESHOLD
                    dfv = df[df['vol'] >= MORE_BTC_THRESHOLD]
                    if not dfv.empty:
                        dfv.set_index('time', drop=True, inplace=True)
                        r = dfv.resample('1min')
                        df1m = r.agg({'close': "last", 'vol': "sum", 'dir': 'max', 'liq': 'min'})
                        df1m = df1m.reset_index().dropna()

                        try:
                            btc_to_add = []
                            for i in range(len(df1m)):
                                btc0 = BTC(df1m.iloc[i, :])
                                if btc0.vol >= MAXIMUM_VOLUME_THRESHOLD:
                                    print(f'{folder1}/{folder2}\t{btc0.time}\t{btc0.close}\t{btc0.vol}')
                                if not session.query(BTC).filter(BTC.time == btc0.time).first():
                                    btc_to_add.append(btc0)
                            session.add_all(btc_to_add)
                            session.commit()
                        except Exception as e:
                            logging.exception(f"Error commit DB: {e}")

        try:
            last_record_time = session.query(BTC).order_by(BTC.time.desc()).first().time + timedelta(hours=3)
            print('Last record(TZ=MSK):', last_record_time)
        except Exception as e:
             logging.exception(f"Error get last record: {e}")
if __name__ == "__main__":
    main()
