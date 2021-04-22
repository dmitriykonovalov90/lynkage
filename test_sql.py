import btoken
import btoken_node
import requests
import json
import db_localhost
import datetime
import db_naumen

global conn, connection
headers = btoken.get_token()
headers_node = btoken_node.get_token()
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


# Показатель рабочие часы факт, tele2_operator_line_work_hours
def test_work_hours_fact_operator():
    url = 'https://node.dev.navigator.lynkage.ru/widgets/content'
    cur = conn.cursor()
    cur_naumen = connection.cursor()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur_naumen.execute(f"select sum(duration) from mv_user_status_period_daily_n where "
    "partner_uuid = 'corebo00000000000lt6e97a8cauvg80' "
    f"and date_work between {first_period} and '2021-04-30' "
    "and (ringing = 'ringing' or speaking = 'speaking' or normal = 'normal' or wrapup = 'wrapup') and away_status_reason is null "
    "and online_status = 'online'")
    work_hours_target = cur_naumen.fetchone()
    print(work_hours_target)

    body = {
        "data_level": "operator",
        "user_id": 8683,
        "widget_acronim": "widget_working_hours_in_line_tele2",
        "parameters":
            {
                "arm": "operator",
                "object_id": 8683,
                "after_date": "2021-04-19 11:16:02",
                "organization": "lcentrix",
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(url, json=body, headers=headers_node)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)

    print('Fact:', requestdict['data']['current']['value'], 'Fact_sql:', work_hours_target)
    print('Percent', requestdict['data']['percent']['value'])
    print('Plan', requestdict['data']['target']['value'])



# закрытие подключения к бд по ssh
def test_stopserver():
    stop(conn)
    stop_naumen(connection)
