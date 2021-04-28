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
first_period = str(first_day_of_current_period)
end_period = str(last_day_of_current_month)

""" Получение текущего года и месяца в формате 202101 для запросов к оракловой бд """
date_current_year = datetime.datetime.now().year
date_current_month = datetime.datetime.now().strftime('%m')
date_current_oracle = str(date_current_year)+str(date_current_month)


@pytest.yield_fixture(autouse=True)
def db_connection():
    global conn, conn_node, server, connection, connection_ora
    conn, conn_node, server = start()
    connection = start_naumen()
    connection_ora = start_oracle()

    yield

    stop(conn, conn_node, server)
    stop_naumen(connection)
    stop_oracle(connection_ora)


@pytest.fixture()
def id_users():
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE login = %s", (login,))
    id_user = cur.fetchone()[0]
    return id_user


@pytest.fixture()
def fio_users():
    cur = conn.cursor()
    cur.execute("SELECT surname, first_name, middle_name FROM users WHERE login = %s", (login,))
    result = cur.fetchall()[0]
    fio_user = (' '.join(result))
    return fio_user


""" **************************** СОЗДАННЫЕ ЗАЯВКИ ФАКТ ************************************ """


@pytest.fixture()
def request_created():
    cur_n_rtk = connection.cursor()

    cur_n_rtk.execute("""
    select sum(calls) from mv_user_calls_result_daily where 
    login = %s and date_work between %s and %s
    and result = 'Согласие клиента'
    """, (login_non_volga, first_period, end_period,))

    result_sql = cur_n_rtk.fetchone()[0]

    if result_sql is None:
        result_sql = 0

    return result_sql


""" ********************************* СОЗДАННЫЕ ЗАЯВКИ ПЛАН ************************************** """


@pytest.fixture()
def request_plan(id_users):
    cur = conn.cursor()
    indicator_id = 2876

    cur.execute("""
    select sum(value) from plan_objects_indicators 
    join objects_indicators on objects_indicators.id = plan_objects_indicators.object_indicator_id 
    join users on users.id = objects_indicators.object_id 
    where indicator_id = %s and users.id = %s and period_begin between %s and %s
    """, (indicator_id, id_users, first_period, end_period,))

    result_sql = cur.fetchone()[0]

    if result_sql is None:
        result_sql = 0
    return result_sql


""" ********************************* КОЛИЧЕСТВО ПОДКЛЮЧЕННЫХ УСЛУГ ФАКТ ************************************** """


@pytest.fixture()
def connected_all_services(fio_users):
    cur_ora_rtk = connection_ora.cursor()

    cur_ora_rtk.execute("""
    select sum(CNT_ALL) from AGP_V_UNITED_REPORT_RES_LN where AGENT = (:1) and PERIOD = (:1)
     """, [fio_users, date_current_oracle])

    result_sql = cur_ora_rtk.fetchone()[0]
    if result_sql is None:
        result_sql = 0
    return result_sql


""" ********************************* КОЛИЧЕСТВО ПОДКЛЮЧЕННЫХ УСЛУГ ПЛАН ************************************** """


@pytest.fixture()
def connect_services_plan(id_users):
    cur = conn.cursor()
    indicator_id = 103

    cur.execute("""
    select sum(value) from plan_objects_indicators 
    join objects_indicators on objects_indicators.id = plan_objects_indicators.object_indicator_id 
    join users on users.id = objects_indicators.object_id 
    where indicator_id = %s and users.id = %s and period_begin between %s and %s
    """, (indicator_id, id_users, first_period, end_period,))

    result_sql = cur.fetchone()[0]
    if result_sql is None:
        result_sql = 0
    return result_sql


""" ********************************* КОЛИЧЕСТВО КОНТАКТОВ ФАКТ ************************************** """


@pytest.fixture()
def contacts_count():
    cur_naumen = connection.cursor()

    cur_naumen.execute("""
    select sum(calls) from mv_user_calls_result_daily
    where login = %s
    and date_work between %s and %s
    and "result" in ('Согласие клиента', 'Отказ клиента')
    """, (login_non_volga, first_period, end_period,))

    result_sql = cur_naumen.fetchone()[0]
    if result_sql is None:
        result_sql = 0
    return result_sql


""" ********************************* КОЛИЧЕСТВО КОНТАКТОВ ФАКТ ************************************** """


@pytest.fixture()
def contacts_count_plan():
    cur = conn.cursor()
    indicator_id = 2879

    cur.execute("""
    select sum(value) from plan_objects_indicators
    join objects_indicators on objects_indicators.id = plan_objects_indicators.object_indicator_id
    join users on users.id = objects_indicators.object_id
    where indicator_id = %s and users.id = %s and period_begin between %s and %s
    """, (indicator_id, id_users(), first_period, end_period,))

    result_sql = cur.fetchone()[0]
    if result_sql is None:
        result_sql = 0
    return result_sql
