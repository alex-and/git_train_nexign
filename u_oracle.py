import cx_Oracle as oracle
import sqlalchemy as sqla
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

import re


def create_oracle_engine(db, pam_secret_name=None, orm=None,
                         pam_secret_path=None):
    '''
    Создать движок/пул подключений к БД Oracle.
    Движок SQLAlchemy (engine) - нужен для скачивания данных из БД.
    Пул подключений cx_Oracle (pool) нужен для загрузки данных в БД.

    :db: название базы данных (доступны DWX, FGEO, ESBTST, ORCL)
    :pam_secret_name: имя аккаунта в PAM
    :orm: ORM, через которую подключаемся (доступны sqlalchemy, cx_Oracle)
    :pam_secret_path: каталог PAM (по умолчанию - персональный)
    '''
    db_list = ['DWX', 'FGEO', 'ESBTST', 'ORCL']
    cred = {'login':'system', 'password':'qq'}
    if cred['login'] == '' or cred['password'] == '':
        print('Не заданы логин или пароль')
        return None
    else:
        pass

    params = {
        'drivername': 'oracle+cx_oracle',
        'username': cred['login'],
        'password': cred['password'],
        'host': None,
        'port': 1521,
        'query': {'encoding': 'utf8'},
    }
    if db not in db_list:
        print(f'Неверно задан параметр db. Допустимые значения параметра db: {db_list}')
        return None
    else:
        pass

    if db == 'DWX':
        params['host'] = 'dwx'
        params['query']['service_name'] = 'bd_dwx'
    elif db == 'FGEO':
        params['host'] = 'vlg-gdc-db'
        params['query']['service_name'] = 'fgeo_prm.megafon.ru'
    elif db == 'ESBTST':
        params['host'] = 'vlg-t-nrioapidb'
        params['query']['service_name'] = 'ESBTST'
    elif db == 'ORCL':
        params['host'] = 'localhost'
        params['query']['service_name'] = 'orcl'
    else:
        pass

    if orm == 'sqlalchemy' or orm is None:
        engine_url = sqla.engine.URL.create(**params)
        engine = sqla.create_engine(engine_url)
        print(f'Создано подключение {pam_secret_name}')
        return engine
    elif orm == 'cx_Oracle':
        dsn = oracle.makedst(
            params['host'],
            params['port'],
            service_name=params['query']['service_name']
        )
        pool = oracle.SessionPool(
            user=cred['login'],
            password=cred['password'],
            dsn=dsn,
            min=2, max=5, increment=1,
            encoding=params['query']['encoding']
        )
        print(f'Создано подключение {pam_secret_name}')
        return pool
    else:
        print('Неверно задан параметр orm')
        return None


