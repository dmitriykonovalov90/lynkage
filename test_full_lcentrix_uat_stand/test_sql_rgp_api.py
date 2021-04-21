import btoken
import requests
import json
import db
import datetime
import db_naumen

global conn, conn_node, server, connection
start = db.startdb
stop = db.stopdb
start_naumen = db_naumen.start_db_naumen
stop_naumen = db_naumen.stop_db_naumen

login = 'vp.peterson2'
client_id = 2
client_secret = '23IzWSgkX5MUlpxSAYJr2o1sM8DRkLXI7vlZFExW'
grant_type = 'password'
organization = 'lcentrix'
password = "OpucK1eZ"
salegroup_id = 59
organization_id = 1
partner_uuid = 'corebo00000000000lt6e97a8cauvg80'
object_type = 'user'
url = 'https://api.uat.navigator.lynkage.ru/formulaConstructor/get'
headers = btoken.get_token(login, password, client_secret, grant_type, client_id, organization)


# Танцы с автоматически получаемыми текущими периодами
def last_day_of_month(date):
    if date.month == 12:
        return date.replace(day=31)
    return date.replace(month=date.month + 1, day=1) - datetime.timedelta(days=1)


date_current = datetime.date.today()  # Текущая дата
yesterday = str(date_current - datetime.timedelta(days=1))
first_day_of_current_period = date_current.replace(day=1)  # Первый день текущего периода
last_day_of_current_month = last_day_of_month(first_day_of_current_period)  # Последний день месяца текущего периода


# старт подключения к бд через ssh
def test_startserver():
    global conn, conn_node, server, connection
    conn, conn_node, server = start()
    connection = start_naumen()


# вычленение id пользователя по имеющемуся логину
def id_users():
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE login = %s", (login,))
    id_user = cur.fetchone()[0]
    return id_user


def uuid_rgp_group():
    cur = conn.cursor()
    user_id = id_users()
    cur.execute('select etl_salegroup_id from salegroups '
                'where foreman_id = %s',
                (user_id,))
    uuid_group = cur.fetchone()
    if uuid_group is None:
        uuid_group = 0
    else:
        uuid_group = uuid_group[0]
    return uuid_group


# Виджет ПРОДАЖИ за месяц план
def test_sales_of_the_month_plan():
    cur = conn.cursor()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'tele2_foreman_lcentrix_connected_services_plan'
    indicator_id = 2647
    cur.execute('select sum(value::float)  from plan_objects_indicators join '
                'objects_indicators on plan_objects_indicators.object_indicator_id = objects_indicators.id '
                'where objects_indicators.object_id = %s '
                'and period_begin between %s and %s '
                'and objects_indicators.indicator_id = %s',
                (salegroup_id, first_day_of_current_period, end_period, indicator_id,))

    result_sql_requests = cur.fetchone()[0]
    if result_sql_requests is None:
        result_sql_requests = 0

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert requestdict['data'][indicator]['value'] == result_sql_requests, "Результат в виджете НЕ соответствует запросу из БД"
    return result_sql_requests


# Виджет ПРОДАЖИ за месяц факт
def test_sales_of_the_month_fact():
    cur_naumen = connection.cursor()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'tele2_operator_lcentrix_connected_services'
    cur_naumen.execute("select coalesce(sum(calls),0) from mv_user_calls_result_daily_n "
                       "where date_work between %s and %s "
                       "and partner_uuid = %s "
                       "and result in ('Услуга подключена', 'Оффер. Услуга подключена') "
                       "and rgp_group_uuid = %s",
                        (first_period, end_period, partner_uuid, uuid_rgp_group(),))

    result_sql_requests = cur_naumen.fetchone()[0]
    if result_sql_requests is None:
        result_sql_requests = 0

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert requestdict['data'][indicator]['value'] == result_sql_requests, "Результат в виджете НЕ соответствует запросу из БД"
    return result_sql_requests


# Виджет ПРОДАЖИ за месяц процент выполнения
def test_sales_of_the_month_percent():
    sales_of_the_month_plan = float(test_sales_of_the_month_plan())
    sales_of_the_month_fact = float(test_sales_of_the_month_fact())
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'tele2_foreman_lcentrix_connected_services_plan_percent'

    if sales_of_the_month_plan == 0:
        result_sql = 0
    else:
        result_sql = round(float(sales_of_the_month_fact / sales_of_the_month_plan * 100), 5)

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    result_method = round(requestdict['data'][indicator]['value'], 5)
    assert result_method == result_sql, "Результат в виджете НЕ соответствует запросу из БД"
    return result_sql


# Виджет рабочие часы за месяц план
def test_work_hours_of_the_month_plan():
    cur = conn.cursor()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'tele2_foreman_lcentrix_work_hours_plan'
    indicator_id = 2726
    cur.execute('select sum(value::int)  from plan_objects_indicators join '
                'objects_indicators on plan_objects_indicators.object_indicator_id = objects_indicators.id '
                'where object_id = %s '
                'and period_begin = %s '
                'and objects_indicators.indicator_id = %s',
                (salegroup_id, first_day_of_current_period, indicator_id,))

    result_sql_requests = cur.fetchone()[0]
    if result_sql_requests is None:
        result_sql_requests = 0

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert requestdict['data'][indicator]['value'] == result_sql_requests, "Результат в виджете НЕ соответствует запросу из БД"
    return result_sql_requests


# Виджет рабочие часы за месяц факт
def test_work_hours_of_the_month_fact():
    cur_naumen = connection.cursor()
    cur = conn.cursor()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    uuid_salegroups = uuid_rgp_group()
    indicator = 'tele2_operator_line_work_hours_current_day'
    cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
                       "where date_work between %s and %s "
                       "and online_status = 'online' "
                       "and partner_uuid = %s "
                       "and away_status_reason is null "
                       "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
                       "and rgp_group_uuid = %s ",
                        (first_period, end_period, partner_uuid, uuid_salegroups,))
    result_sql_requests = cur_naumen.fetchone()[0]
    if result_sql_requests is None:
        result_sql_requests = 0
    cur.execute("SELECT coalesce(sum(value)) from lcentrix_idle_hours join users on users.id = user_id "
                "where salegroup_id = %s and date between %s and %s", (salegroup_id, first_period, end_period,))
    hours_penalty = cur.fetchone()[0]
    if hours_penalty is None:
        hours_penalty = 0
    total_result_sql = result_sql_requests - hours_penalty

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert requestdict['data'][indicator]['value'] == total_result_sql, "Результат в виджете НЕ соответствует запросу из БД"
    return total_result_sql


# Виджет рабочие часы за месяц процент выполнения
def test_work_hours_of_the_month_percent():
    work_hours_of_the_month_fact = test_work_hours_of_the_month_fact()
    work_hours_of_the_month_plan = test_work_hours_of_the_month_plan()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'tele2_foreman_lcentrix_work_hours_plan_percent'

    if work_hours_of_the_month_plan == 0:
        result_sql = 0
    else:
        result_sql = round(float(work_hours_of_the_month_fact / work_hours_of_the_month_plan * 100), 5)

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    result_method = round(requestdict['data'][indicator]['value'], 5)
    assert result_method == result_sql, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет продажи на текущий день план
def test_sales_of_the_current_period_plan():
    cur = conn.cursor()
    first_period = str(first_day_of_current_period)
    current_day = str(date_current)
    indicator = 'tele2_services_connected_current_day_plan'
    indicator_id = 2641
    cur.execute('select sum(value::float)  from plan_objects_indicators join '
                'objects_indicators on plan_objects_indicators.object_indicator_id = objects_indicators.id '
                'join users on objects_indicators.object_id = users.id '
                'where users.salegroup_id = %s '
                'and period_begin between %s and %s '
                'and objects_indicators.indicator_id = %s',
                (salegroup_id, first_day_of_current_period, current_day, indicator_id,))

    result_sql_requests = cur.fetchone()[0]
    if result_sql_requests is None:
        result_sql_requests = 0

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": current_day
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert round(requestdict['data'][indicator]['value'], 5) == round(result_sql_requests, 5), "Результат в виджете НЕ соответствует запросу из БД"
    return result_sql_requests


# Виджет ПРОДАЖИ на текущий день процент выполнения
def test_sales_on_the_current_period_percent():
    sales_on_the_current_period_plan = float(test_sales_of_the_current_period_plan())
    sales_of_the_month_fact = float(test_sales_of_the_month_fact())
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'tele2_services_connected_current_day_plan_percent'

    if sales_on_the_current_period_plan == 0:
        result_sql = 0
    else:
        result_sql = round(float(sales_of_the_month_fact / sales_on_the_current_period_plan * 100), 5)

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    result_method = round(requestdict['data'][indicator]['value'], 5)
    assert result_method == result_sql, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет рабочие часы на текущий день план
def test_work_hours_on_the_current_period_plan():
    cur = conn.cursor()
    first_period = str(first_day_of_current_period)
    indicator = 'tele2_operator_line_work_hours_current_day_plan'
    indicator_id = 1990
    current_day = str(date_current)
    cur.execute('select sum(value::int)  from plan_objects_indicators join '
                'objects_indicators on plan_objects_indicators.object_indicator_id = objects_indicators.id '
                'join users on objects_indicators.object_id = users.id '
                'where users.salegroup_id = %s '
                'and period_begin between %s and %s '
                'and objects_indicators.indicator_id = %s',
                (salegroup_id, first_day_of_current_period, current_day, indicator_id,))

    result_sql_requests = cur.fetchone()[0]
    if result_sql_requests is None:
        result_sql_requests = 0

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": current_day
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert requestdict['data'][indicator]['value'] == result_sql_requests, "Результат в виджете НЕ соответствует запросу из БД"
    return result_sql_requests


# Виджет РАБОЧИЕ ЧАСЫ на текущий день процент выполнения
def test_work_hours_on_the_current_period_percent():
    work_hours_on_the_current_period_fact = float(test_work_hours_of_the_month_fact())
    work_hours_on_the_current_period_plan = float(test_work_hours_on_the_current_period_plan())
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'tele2_operator_line_work_hours_current_day_plan_percent'

    if work_hours_on_the_current_period_plan == 0:
        result_sql = 0
    else:
        result_sql = round(float(work_hours_on_the_current_period_fact / work_hours_on_the_current_period_plan * 100), 5)

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    result_method = round(requestdict['data'][indicator]['value'], 5)
    assert result_method == result_sql, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет AHT
def test_aht():
    cur_naumen = connection.cursor()
    cur = conn.cursor()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    uuid_salegroups = uuid_rgp_group()
    indicator = 'tele2_ant'
    current_day = str(date_current)
    cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
                       "where date_work between %s and %s "
                       "and partner_uuid = %s "
                       "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
                       "and away_status_reason is null "
                       "and rgp_group_uuid = %s ",
                        (first_period, end_period, partner_uuid, uuid_salegroups,))
    result_sql_workhours = cur_naumen.fetchone()[0]

    cur.execute("SELECT coalesce(sum(value)) from lcentrix_idle_hours join users on users.id = user_id "
                "where salegroup_id = %s and date between %s and %s", (salegroup_id, first_period, end_period,))
    hours_penalty = cur.fetchone()[0]
    if hours_penalty is None:
        hours_penalty = 0
    total_result_sql = (result_sql_workhours - hours_penalty) / 60

    cur_naumen.execute(
        "select coalesce(sum(calls),0) from mv_user_calls_result_daily_n "
        "where date_work between %s and %s "
        "and partner_uuid = %s "
        "and (result not in ('Автоответчик', 'Не берут трубку', 'Неправильный номер'))"
        "and rgp_group_uuid = %s",
        (first_period, end_period, partner_uuid, uuid_rgp_group(),))

    result_sql_talking = cur_naumen.fetchone()[0]
    if result_sql_talking is None:
        result_sql = 0
    else:
        result_sql = float(total_result_sql / result_sql_talking)

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": current_day
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert round(requestdict['data'][indicator]['value'], 5) == round(result_sql, 5), "Результат в виджете НЕ соответствует запросу из БД"


# Виджет рабочие часы ЗА ТЕКУЩИЙ ДЕНЬ план
def test_work_hours_on_the_current_day_plan():
    cur = conn.cursor()
    current_day = str(date_current)
    indicator = 'tele2_operator_line_work_hours_this_day_plan'
    indicator_id = 1990
    cur.execute('select sum(value::int)  from plan_objects_indicators join '
                'objects_indicators on plan_objects_indicators.object_indicator_id = objects_indicators.id '
                'join users on objects_indicators.object_id = users.id '
                'where users.salegroup_id = %s '
                'and period_begin = %s '
                'and objects_indicators.indicator_id = %s',
                (salegroup_id, current_day, indicator_id,))

    result_sql_requests = cur.fetchone()[0]
    if result_sql_requests is None:
        result_sql_requests = 0

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": current_day,
                "period_end": current_day
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert requestdict['data'][indicator]['value'] == result_sql_requests, "Результат в виджете НЕ соответствует запросу из БД"
    return result_sql_requests


# Виджет рабочие часы ЗА ТЕКУЩИЙ ДЕНЬ факт
def test_work_hours_of_the_current_day_fact():
    cur_naumen = connection.cursor()
    cur = conn.cursor()
    current_day = str(date_current)
    indicator = 'tele2_operator_line_work_hours_this_day'
    uuid_salegroups = uuid_rgp_group()
    cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
                       "where date_work = %s "
                       "and partner_uuid = %s "
                       "and online_status = 'online' "
                       "and away_status_reason is null "
                       "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
                       "and rgp_group_uuid = %s ",
                        (current_day, partner_uuid, uuid_salegroups,))

    result_sql_requests = cur_naumen.fetchone()[0]
    if result_sql_requests is None:
        result_sql_requests = 0
    cur.execute("SELECT coalesce(sum(value)) from lcentrix_idle_hours join users on users.id = user_id "
                "where salegroup_id = %s and date = %s", (salegroup_id, current_day,))
    hours_penalty = cur.fetchone()[0]
    if hours_penalty is not None:
        result_sql_requests = result_sql_requests - hours_penalty

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": current_day,
                "period_end": current_day
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert requestdict['data'][indicator]['value'] == int(result_sql_requests), "Результат в виджете НЕ соответствует запросу из БД"
    return result_sql_requests


# Виджет рабочие часы за ТЕКУЩИЙ ДЕНЬ процент выполнения
def test_work_hours_of_the_current_day_percent():
    work_hours_of_the_current_day_fact = test_work_hours_of_the_current_day_fact()
    work_hours_of_the_current_day_plan = test_work_hours_on_the_current_day_plan()
    current_day = str(date_current)
    indicator = 'tele2_operator_line_work_hours_this_day_plan_percent'

    if work_hours_of_the_current_day_plan == 0:
        result_sql = 0
    else:
        result_sql = round(float(work_hours_of_the_current_day_fact / work_hours_of_the_current_day_plan * 100), 5)

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": current_day,
                "period_end": current_day
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    result_method = round(requestdict['data'][indicator]['value'], 5)
    assert result_method == result_sql, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет ПРОДАЖИ за ТЕКУЩИЙ ДЕНЬ план
def test_sales_of_the_current_day_plan():
    cur = conn.cursor()
    current_day = str(date_current)
    indicator = 'tele2_services_connected_this_day_plan'
    indicator_id = 2641
    cur.execute('select sum(value::float)  from plan_objects_indicators join '
                'objects_indicators on plan_objects_indicators.object_indicator_id = objects_indicators.id '
                'join users on objects_indicators.object_id = users.id '
                'where users.salegroup_id = %s '
                'and period_begin between %s and %s '
                'and objects_indicators.indicator_id = %s',
                (salegroup_id, current_day, current_day, indicator_id,))

    result_sql_requests = cur.fetchone()[0]
    if result_sql_requests is None:
        result_sql_requests = 0

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": current_day,
                "period_end": current_day
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert requestdict['data'][indicator]['value'] == result_sql_requests, "Результат в виджете НЕ соответствует запросу из БД"
    return result_sql_requests


# Виджет ПРОДАЖИ за ТЕКУЩИЙ ДЕНЬ факт
def test_sales_of_the_current_day_fact():
    cur_naumen = connection.cursor()
    current_day = str(date_current)
    indicator = 'tele2_services_connected_this_day'
    cur_naumen.execute("select coalesce(sum(calls),0) from mv_phone_call_today "
                       "where date_work = %s "
                       "and partner_uuid = %s "
                       "and result in ('Услуга подключена', 'Оффер. Услуга подключена') "
                       "and rgp_group_uuid = %s",
                        (current_day, partner_uuid, uuid_rgp_group(),))

    result_sql_requests = cur_naumen.fetchone()[0]
    if result_sql_requests is None:
        result_sql_requests = 0

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": current_day,
                "period_end": current_day
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert requestdict['data'][indicator]['value'] == result_sql_requests, "Результат в виджете НЕ соответствует запросу из БД"
    return result_sql_requests


# Виджет ПРОДАЖИ за месяц процент выполнения
def test_sales_of_the_current_day_percent():
    sales_of_the_current_day_plan = float(test_sales_of_the_current_day_plan())
    sales_of_the_current_day_fact = float(test_sales_of_the_current_day_fact())
    current_day = str(date_current)
    indicator = 'tele2_services_connected_this_day_plan_percent'

    if sales_of_the_current_day_plan == 0:
        result_sql = 0
    else:
        result_sql = round(float(sales_of_the_current_day_fact / sales_of_the_current_day_plan * 100), 5)

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": current_day,
                "period_end": current_day
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    result_method = round(requestdict['data'][indicator]['value'], 5)
    assert result_method == result_sql, "Результат в виджете НЕ соответствует запросу из БД"


# прогноз дохода на текущий день
def test_rgp_salary():
    first_period = str(first_day_of_current_period)
    indicator = 'tele2_foreman_salary'
    current_day = str(date_current)
    rate_main = 22600

    sales_to_the_month_fact = float(test_sales_of_the_month_fact())
    work_hours_to_the_month_fact = float(test_work_hours_of_the_month_fact() / 3600)
    sales_of_the_month_plan = float(test_sales_of_the_month_plan())

    kpi = sales_to_the_month_fact / work_hours_to_the_month_fact
    # вычисление ставки за услуги
    if kpi < 2.5:
        rate_service = 0
    elif kpi < 2.8:
        rate_service = 1.25
    elif kpi < 3.1:
        rate_service = 1.8
    elif kpi < 3.4:
        rate_service = 2.45
    elif kpi < 4:
        rate_service = 3.2
    elif kpi < 4.3:
        rate_service = 4.05
    else:
        rate_service = 5.5

    # вычисление коэффициента выполнения плана
    if test_sales_of_the_month_plan() == 0:
        kpi_sales = 0
    else:
        kpi_sales = float(sales_to_the_month_fact) / float(test_sales_of_the_month_plan())

    # Считать премию ТОЛЬКО после того, как выполнено 85% плана
    implementation_of_a_plan = sales_to_the_month_fact / sales_of_the_month_plan
    if implementation_of_a_plan >= 0.85:
        implementation_of_a_plan = 1
    else:
        implementation_of_a_plan = 0

    # ЗП = продажи за месяц факт * ставку за услуги * кэф по продажам * конверсия (пока считаем её 1) + оклад
    result_sql_salary = round(float(sales_to_the_month_fact) * float(rate_service) * kpi_sales * 1 * implementation_of_a_plan + rate_main, 0)

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": current_day
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert requestdict['data'][indicator]['value'] == result_sql_salary, "Результат в виджете НЕ соответствует запросу из БД"


# прогноз дохода при выполнении плана по заявкам
def test_rgp_salary_forecast():
    first_period = str(first_day_of_current_period)
    indicator = 'tele2_foreman_salary_forecast'
    current_day = str(date_current)
    rate_main = 22600
    sales_on_the_month_plan = float(test_sales_of_the_month_plan())

    sales_to_the_month_fact = float(test_sales_of_the_month_fact())
    work_hours_to_the_month_fact = float(test_work_hours_of_the_month_fact()) / 3600

    kpi = sales_to_the_month_fact / work_hours_to_the_month_fact
    # вычисление ставки за услуги
    if kpi < 2.5:
        rate_service = 0
    elif kpi < 2.8:
        rate_service = 1.25
    elif kpi < 3.1:
        rate_service = 1.8
    elif kpi < 3.4:
        rate_service = 2.45
    elif kpi < 4:
        rate_service = 3.2
    elif kpi < 4.3:
        rate_service = 4.05
    else:
        rate_service = 5.5

    result_sql_salary_forecast = round(sales_on_the_month_plan * rate_service + rate_main, 0)

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": current_day
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert requestdict['data'][indicator]['value'] == result_sql_salary_forecast, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет ЭФФЕКТИВНОСТЬ СПИДОМЕТРЫ заявок в час
def test_efficiency_speed_connect_services_in_hours():
    sales_on_the_month_fact = float(test_sales_of_the_month_fact())
    work_hours_on_the_month_fact = float(test_work_hours_of_the_month_fact())
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'tele2_efficiency'

    result_sql = sales_on_the_month_fact / (work_hours_on_the_month_fact / 3600)

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    result_method = requestdict['data'][indicator]['value']
    assert round(result_method, 5) == round(result_sql, 5), "Результат в виджете НЕ соответствует запросу из БД"


# Виджет ЭФФЕКТИВНОСТЬ СПИДОМЕТРЫ обработка
def test_efficiency_speed_processing_talk_time():
    sales_on_the_month_fact = float(test_sales_of_the_month_fact())
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'tele2_foreman_services_connected_average_talk_time'
    cur_naumen = connection.cursor()
    cur_naumen.execute("select coalesce(sum(talk_time),0) from mv_user_calls_result_daily_n "
                       "where date_work between %s and %s "
                       "and partner_uuid = %s "
                       "and result in ('Услуга подключена', 'Оффер. Услуга подключена') "
                       "and rgp_group_uuid = %s",
                        (first_period, end_period, partner_uuid, uuid_rgp_group(),))

    work_hours_on_the_month_fact_in_sales = cur_naumen.fetchone()
    if work_hours_on_the_month_fact_in_sales is None:
        work_hours_on_the_month_fact_in_sales = 0
    else:
        work_hours_on_the_month_fact_in_sales = float(work_hours_on_the_month_fact_in_sales[0]) / 60

    if sales_on_the_month_fact == 0:
        result_sql = 0
    else:
        result_sql = work_hours_on_the_month_fact_in_sales / sales_on_the_month_fact

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    result_method = requestdict['data'][indicator]['value']
    assert result_method == result_sql, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет занятость - занятость
def test_employment_work():
    cur_naumen = connection.cursor()
    cur = conn.cursor()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    uuid_salegroups = uuid_rgp_group()
    indicator = 'lcentrix_employment_detail_work'
    cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
                       "where date_work between %s and %s "
                       "and partner_uuid = %s "
                       "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
                       "and away_status_reason is null "
                       "and rgp_group_uuid = %s",
                        (first_period, end_period, partner_uuid, uuid_salegroups,))

    result_sql_work = cur_naumen.fetchone()[0]
    cur.execute("SELECT coalesce(sum(value)) from lcentrix_idle_hours join users on users.id = user_id "
                "where salegroup_id = %s and date between %s and %s", (salegroup_id, first_period, end_period,))
    hours_penalty = cur.fetchone()[0]
    if hours_penalty is None:
        hours_penalty = 0
    result_sql_work = result_sql_work - hours_penalty

    cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
                       "where date_work between %s and %s "
                       "and partner_uuid = %s "
                       "and away_status_reason = 'CustomAwayReason2' "
                       "and rgp_group_uuid = %s",
                        (first_period, end_period, partner_uuid, uuid_salegroups,))

    result_sql_away = cur_naumen.fetchone()[0]

    result_100_employment = result_sql_work + result_sql_away

    result_sql = round(float(result_sql_work * 100 / result_100_employment), 2)

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert requestdict['data'][indicator]['value']['value'] == result_sql, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет занятость - ОТДЫХ
def test_employment_away():
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'lcentrix_employment_detail_away'
    cur_naumen = connection.cursor()
    cur = conn.cursor()
    uuid_salegroups = uuid_rgp_group()
    cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
                       "where date_work between %s and %s "
                       "and partner_uuid = %s "
                       "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
                       "and away_status_reason is null "
                       "and rgp_group_uuid = %s",
                       (first_period, end_period, partner_uuid, uuid_salegroups,))

    result_sql_work = cur_naumen.fetchone()[0]
    cur.execute("SELECT coalesce(sum(value)) from lcentrix_idle_hours join users on users.id = user_id "
                "where salegroup_id = %s and date between %s and %s", (salegroup_id, first_period, end_period,))
    hours_penalty = cur.fetchone()[0]
    if hours_penalty is None:
        hours_penalty = 0
    result_sql_work = result_sql_work - hours_penalty
    cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
                       "where date_work between %s and %s "
                       "and partner_uuid = %s "
                       "and away_status_reason = 'CustomAwayReason2' "
                       "and rgp_group_uuid = %s",
                       (first_period, end_period, partner_uuid, uuid_salegroups,))

    result_sql_away = cur_naumen.fetchone()[0]

    result_100_employment = result_sql_work + result_sql_away

    result_sql = round(float(result_sql_away * 100 / result_100_employment), 2)

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert requestdict['data'][indicator]['value']['value'] == result_sql, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет занятость - ОТДЫХ
def test_employment_training():
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'lcentrix_employment_detail_training'
    hardcode_value = 0
    # cur_naumen = connection.cursor()
    # cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n c "
    #                    "where c.date_work between %s and %s "
    #                    "and partner_uuid = %s "
    #                    "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
    #                    "and away_status_reason is null "
    #                    "and c.login in "
    #                    "(select p.login from dblink"
    #                    "('host=192.168.77.123 dbname=api user=dashboard_user password=password',"
    #                    " 'select login from users where salegroup_id = %s') "
    #                    "as p (login varchar(256)))",
    #                    (first_period, end_period, partner_uuid, salegroup_id,))
    #
    # result_sql_work = cur_naumen.fetchone()[0]
    #
    # cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n c "
    #                    "where c.date_work between %s and %s "
    #                    "and partner_uuid = %s "
    #                    "and away_status_reason = 'CustomAwayReason2' "
    #                    "and c.login in "
    #                    "(select p.login from dblink"
    #                    "('host=192.168.77.123 dbname=api user=dashboard_user password=password',"
    #                    " 'select login from users where salegroup_id = %s') "
    #                    "as p (login varchar(256)))",
    #                    (first_period, end_period, partner_uuid, salegroup_id,))
    #
    # result_sql_away = cur_naumen.fetchone()[0]
    #
    # cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n c "
    #                    "where c.date_work between %s and %s "
    #                    "and partner_uuid = %s "
    #                    "and away_status_reason = 'CustomAwayReason1' "
    #                    "and c.login in "
    #                    "(select p.login from dblink"
    #                    "('host=192.168.77.123 dbname=api user=dashboard_user password=password',"
    #                    " 'select login from users where salegroup_id = %s') "
    #                    "as p (login varchar(256)))",
    #                    (first_period, end_period, partner_uuid, salegroup_id,))

    # result_sql_training = cur_naumen.fetchone()[0]
    #
    # result_100_employment = result_sql_work + result_sql_training + result_sql_away
    #
    # result_sql = round(float(result_sql_training * 100 / result_100_employment), 2)

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    assert requestdict['data'][indicator]['value']['value'] == hardcode_value, "Результат в виджете НЕ соответствует запросу из БД"


def get_array_sales_everyday_fact(day):
    cur_naumen = connection.cursor()
    array_fact = []
    date_current_string = str(date_current)

    if day == date_current_string:
        cur_naumen.execute("select coalesce(sum(calls),0) from mv_phone_call_today "
                           "where partner_uuid = %s "
                           "and result in ('Услуга подключена', 'Оффер. Услуга подключена') "
                           "and rgp_group_uuid = %s",
                           (partner_uuid, uuid_rgp_group(),))
        array_fact.append(float(cur_naumen.fetchone()[0]))
    else:
        cur_naumen.execute("select coalesce(sum(calls),0) from mv_user_calls_result_daily_n "
                               "where date_work = %s "
                               "and partner_uuid = %s "
                               "and result in ('Услуга подключена', 'Оффер. Услуга подключена') "
                               "and rgp_group_uuid = %s",
                                (day, partner_uuid, uuid_rgp_group(),))
        array_fact.append(float(cur_naumen.fetchone()[0]))
    return array_fact


def get_array_work_hours_everyday_fact(day):
    cur_naumen = connection.cursor()
    cur = conn.cursor()
    array_fact = []
    uuid_salegroups = uuid_rgp_group()
    cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
                               "where date_work = %s "
                               "and partner_uuid = %s "
                               "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
                               "and away_status_reason is null "
                               "and online_status = 'online' "
                               "and rgp_group_uuid = %s",
                                (day, partner_uuid, uuid_salegroups,))
    result = cur_naumen.fetchone()[0]
    if result is None:
        result = 0
    cur.execute("SELECT coalesce(sum(value)) from lcentrix_idle_hours join users on users.id = user_id "
                "where salegroup_id = %s and date = %s", (salegroup_id, day,))
    hours_penalty = cur.fetchone()[0]
    if hours_penalty is None:
        hours_penalty = 0
    total_result_sql = result - hours_penalty
    array_fact.append(int(total_result_sql))
    return array_fact


def get_array_sales_everyday_plan(day):
    cur = conn.cursor()
    array_fact = []
    indicator_id = 2662

    cur.execute('select sum(value::float)  from plan_objects_indicators join '
                    'objects_indicators on plan_objects_indicators.object_indicator_id = objects_indicators.id '
                    'join users on objects_indicators.object_id = users.id '
                    'where users.salegroup_id = %s '
                    'and period_begin = %s '
                    'and objects_indicators.indicator_id = %s',
                    (salegroup_id, day, indicator_id,))
    result = cur.fetchone()[0]
    if result is None:
        result = 0
    array_fact.append(int(result))
    return array_fact


def get_array_working_hours_everyday_plan(day):
    cur = conn.cursor()
    array_fact = []
    indicator_id = 1990

    cur.execute('select sum(value::float)  from plan_objects_indicators join '
                    'objects_indicators on plan_objects_indicators.object_indicator_id = objects_indicators.id '
                    'join users on objects_indicators.object_id = users.id '
                    'where users.salegroup_id = %s '
                    'and period_begin = %s '
                    'and objects_indicators.indicator_id = %s',
                    (salegroup_id, day, indicator_id,))
    result = cur.fetchone()[0]
    if result is None:
        result = 0
    array_fact.append(int(result))
    return array_fact


# Виджет Рабочее время
def test_working_hours_graph():
    global first_day_of_current_period
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'tele2_operator_line_work_hours_daily_graph'

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)

    # Сравнение РАБОЧИХ ЧАСОВ ФАКТОВ ЗА КАЖДЫЙ ДЕНЬ
    iteration = 1
    first_day_of_current_period = date_current.replace(day=1)
    while iteration <= last_day_of_current_month.day:
        first_period = str(first_day_of_current_period)
        work_hours_fact_sql = get_array_work_hours_everyday_fact(first_period)
        if work_hours_fact_sql[0] != 0:
            assert work_hours_fact_sql[0] == requestdict['data'][indicator]['value'][first_period]['fact']
        first_day_of_current_period = first_day_of_current_period + datetime.timedelta(days=+1)
        iteration = iteration + 1

    # Сравнение ПРОДАЖ ФАКТОВ ЗА КАЖДЫЙ ДЕНЬ
    iteration = 1
    first_day_of_current_period = date_current.replace(day=1)
    while iteration <= date_current.day:
        first_period = str(first_day_of_current_period)
        sales_fact_sql = get_array_sales_everyday_fact(first_period)
        if sales_fact_sql[0] != 0:
            assert sales_fact_sql[0] == requestdict['data'][indicator]['value'][first_period]['fact_min_production']
        first_day_of_current_period = first_day_of_current_period + datetime.timedelta(days=+1)
        iteration = iteration + 1

    # Сравнение РАБОЧИХ ЧАСОВ ФАКТОВ ЗА КАЖДЫЙ ПЛАН
    iteration = 1
    first_day_of_current_period = date_current.replace(day=1)
    while iteration <= last_day_of_current_month.day:
        first_period = str(first_day_of_current_period)
        work_hours_plan_sql = get_array_working_hours_everyday_plan(first_period)
        if work_hours_plan_sql[0] != 0:
            assert work_hours_plan_sql[0] == requestdict['data'][indicator]['value'][first_period]['plan']
        first_day_of_current_period = first_day_of_current_period + datetime.timedelta(days=+1)
        iteration = iteration + 1

    # Сравнение ПРОДАЖ ЗА КАЖДЫЙ ПЛАН
    iteration = 1
    first_day_of_current_period = date_current.replace(day=1)
    while iteration <= last_day_of_current_month.day:
        first_period = str(first_day_of_current_period)
        sales_plan_sql = get_array_sales_everyday_plan(first_period)
        if sales_plan_sql[0] != 0:
            assert sales_plan_sql[0] == requestdict['data'][indicator]['value'][first_period]['plan_min_production']
        first_day_of_current_period = first_day_of_current_period + datetime.timedelta(days=+1)
        iteration = iteration + 1


# Виджет эффективность графический
def test_efficiency_daily_graph():
    global first_day_of_current_period
    first_period = str(date_current.replace(day=1))
    end_period = str(last_day_of_current_month)
    indicator = 'tele2_operator_efficiency_daily_graph'

    body = {
            "indicator_acronim": indicator,
            "object_id": salegroup_id,
            "object_type": 'salegroup',
            "parameters": {
                "organization_id": organization_id,
                "period_begin": first_period,
                "period_end": end_period
            }
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)

    iteration = 1
    first_day_of_current_period = date_current.replace(day=1)
    while iteration <= date_current.day:
        first_period = str(first_day_of_current_period)
        sales_fact_sql = get_array_sales_everyday_fact(first_period)
        work_hours_fact_sql = get_array_work_hours_everyday_fact(first_period)
        if work_hours_fact_sql[0] == 0:
            result_efficiency_sql = 0
        else:
            result_efficiency_sql = sales_fact_sql[0] / (work_hours_fact_sql[0] / 3600)
        if result_efficiency_sql != 0:
            assert round(result_efficiency_sql, 5) == round(requestdict['data'][indicator]['value'][first_period]['fact'], 5)
        first_day_of_current_period = first_day_of_current_period + datetime.timedelta(days=+1)
        iteration = iteration + 1


# закрытие подключения к бд по ssh
def test_stopserver():
    stop(conn, conn_node, server)
    stop_naumen(connection)
