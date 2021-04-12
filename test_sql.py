import btoken
import requests
import json
import db_localhost
import datetime
import db_naumen

global conn, connection
headers = btoken.get_token()
start = db_localhost.start_db_localhost
stop = db_localhost.stop_db_localhost
start_naumen = db_naumen.start_db_naumen
stop_naumen = db_naumen.stop_db_naumen

LOGIN = 'ta.yakobson1'
ORGANIZATION_ID = 1
OBJECT_TYPE = 'organization'
URL = 'http://localhost/formulaConstructor/get'
PARTNER_UUID = 'corebo00000000000lt6e97a8cauvg80'


# Танцы с автоматически получаемыми текущими периодами
def last_day_of_month(date):
    if date.month == 12:
        return date.replace(day=31)
    return date.replace(month=date.month + 1, day=1) - datetime.timedelta(days=1)


date_current = datetime.date.today()  # Текущая дата
first_day_of_current_period = date_current.replace(day=1)  # Первый день текущего периода
last_day_of_current_month = last_day_of_month(first_day_of_current_period)  # Последний день месяца текущего периода
date_yesterday = datetime.date.today() + datetime.timedelta(days=-1)  # Вчерашняя дата

# Вычисление коэффициента: последний день месяца/предыдущий день
day_of_the_yesterday = date_yesterday.day  # вчерашний день
last_day_day = last_day_of_current_month.day  # последний день месяца
kpi_yesterday_and_last_day = last_day_day / day_of_the_yesterday


# старт подключения к бд через ssh
def test_startserver():
    global conn, connection
    conn = start()
    connection = start_naumen()


# вычленение id пользователя по имеющемуся логину
def id_users():
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE login = %s", (LOGIN,))
    id_user = cur.fetchone()[0]
    return id_user


'''
графический виджет рабочего времени. Состоит из 6 показателей. Обращение идёт как в организации в целом
так и к площадке по отдельности
'''


# Показатель рабочие часы факт, tele2_operator_line_work_hours
def test_work_hours_fact():
    cur = conn.cursor()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'tele2_operator_line_work_hours'
    cur_etl = connection.cursor()
    cur_etl.execute("select sum(duration) from mv_user_status_period_daily_n where "
                    "partner_uuid = %s "
                    "and date_work between %s and %s "
                    "and (ringing = 'ringing' or speaking = 'speaking' or normal = 'normal' or wrapup = 'wrapup') "
                    "and away_status_reason is null and online_status = 'online'",
                    (PARTNER_UUID, first_day_of_current_period, last_day_of_current_month, ))
    result_sql_requests = cur_etl.fetchone()

    body = {
            "indicator_acronim": indicator,
            "object_id": ORGANIZATION_ID,
            "object_type": OBJECT_TYPE,
            "parameters": {
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(URL, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert requestdict['data'][indicator]['value'] == result_sql_requests, "Результат в виджете НЕ соответствует запросу из БД"



# закрытие подключения к бд по ssh
def test_stopserver():
    stop(conn)
    stop_naumen(connection)
