import pytest

import btoken
import requests
import json
import db
import datetime
import db_n_rtk
import db_oracle


global conn, conn_node, server, connection, connection_ora
start = db.startdb
stop = db.stopdb
start_naumen = db_n_rtk.start_db_naumen
stop_naumen = db_n_rtk.stop_db_naumen
start_oracle = db_oracle.start_db_oracle
stop_oracle = db_oracle.stop_db_oracle

login = '13050045Trostina_volga'
login_non_volga = '13050045Trostina'
password = "123456"
client_id = 2
client_secret = '23IzWSgkX5MUlpxSAYJr2o1sM8DRkLXI7vlZFExW'
grant_type = 'password'
organization = 'rtk'
salegroup_id = 60
organization_id = 3
partner_uuid = 'corebo00000000000lt6e97a8cauvg80'
object_type = 'user'
url = 'https://api.test.navigator.lynkage.ru/formulaConstructor/get'
headers = btoken.get_token(login, password, client_secret, grant_type, client_id, organization)


# Танцы с автоматически получаемыми текущими периодами
def last_day_of_month(date):
    if date.month == 12:
        return date.replace(day=31)
    return date.replace(month=date.month + 1, day=1) - datetime.timedelta(days=1)


date_current = datetime.date.today()  # Текущая дата
first_day_of_current_period = date_current.replace(day=1)  # Первый день текущего периода
last_day_of_current_month = last_day_of_month(first_day_of_current_period)  # Последний день месяца текущего периода

""" Получение текущего года и месяца в формате 202101 для запросов к оракловой бд """
date_current_year = datetime.datetime.now().year
date_current_month = datetime.datetime.now().strftime('%m')
date_current_oracle = str(date_current_year)+str(date_current_month)



@pytest.yield_fixture(autouse=True)
def dbstart():
    global conn, conn_node, server, connection, connection_ora
    conn, conn_node, server = start()
    connection = start_naumen()
    connection_ora = start_oracle()

    yield

    stop(conn, conn_node, server)
    stop_naumen(connection)
    stop_oracle(connection_ora)

@pytest.fixture()
def request_created():
    cur_n_rtk = connection.cursor()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    result = 'Согласие клиента'

    cur_n_rtk.execute('select sum(calls) from mv_user_calls_result_daily where '
                      'login = %s and date_work between %s and %s'
                      'and result = %s',
                      (login_non_volga, first_period, end_period, result,))

    result_sql_requests = cur_n_rtk.fetchone()[0]

    if result_sql_requests is None:
        result_sql_requests = 0

    return result_sql_requests

# def id_users():
#     cur = conn.cursor()
#     cur.execute("SELECT id FROM users WHERE login = %s", (login,))
#     id_user = cur.fetchone()[0]
#     return id_user


# закрытие подключения к бд по ssh

def stopserver():
    stop(conn, conn_node, server)
    stop_naumen(connection)
    stop_oracle(connection_ora)
