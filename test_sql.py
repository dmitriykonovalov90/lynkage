import btoken
import requests
import json
import db
import datetime
import db_naumen

global conn, server, connection
headers = btoken.get_token()
start = db.startdb
stop = db.stopdb
start_naumen = db_naumen.start_db_naumen
stop_naumen = db_naumen.stop_db_naumen

login = 'g.starichenkova'
organization_id = 3
object_type = 'user'
url = 'https://api.test.navigator.lynkage.ru/formulaConstructor/get'

# id услуг
# Основные
service_id_internet = 37
service_id_iptv = 34
service_id_wink = 43
service_id_telephone = 1246
# Допы
service_id_internal_camera = 1248
service_id_external_camera = 1249
service_id_mobile = 35
service_id_2_in_1 = 1250
service_id_3_in_1 = 1252

# Ставки за услуги:
rate_internet = 500
rate_iptv = 400
rate_wink = 400
rate_telephone = 70
rate_internal_camera = 600
rate_external_camera = 700
rate_2_in_1 = 1000
rate_3_in_1 = 1800
rate_mobile = 35

# Удельный вес
share_of_implementation_kpi_qm = 0.3  # Удельный вес выполнения качества прослушанных диалогов
share_of_implementation_kpi_plan_main_connect_service = 0.2  # Удельный вес кэф по подключенным основным услугам
share_of_implementation_kpi_conversion_main = 0.2  # Удельный вес выполнения конвертации по основным услугам
share_of_implementation_kpi_plan_slave_connect_service = 0.15  # Удельный вес кэф по подключенным ДОП услугам
share_of_implementation_kpi_conversion_slave = 0.15  # Удельный вес выполнения конвертации по ДОП услугам


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
    global conn, server, connection
    conn, server = start()
    connection = start_naumen()


# вычленение id пользователя по имеющемуся логину
def id_users():
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE login = %s", (login,))
    id_user = cur.fetchone()[0]
    return id_user


def rate_of_the_positions():
    user_id = id_users()
    cur = conn.cursor()
    # director_position = 1
    operator_position = 2
    probationer_position = 4
    senior_operator_position = 5
    # rgp_position = 3
    # platform_director_position = 6
    cur.execute("SELECT position_id FROM users_positions WHERE user_id = %s", (user_id,))
    id_position = cur.fetchone()[0]
    if id_position == probationer_position:
        rate_the_bablo = 45
    elif id_position == operator_position:
        rate_the_bablo = 80
    elif id_position == senior_operator_position:
        rate_the_bablo = 110
    else:
        rate_the_bablo = 0

    return rate_the_bablo


# Виджет заявки ШПД-ТВ, тест на соответствие фактического показателя по заявкам
def test_main_requests_fact_current_month():
    cur = conn.cursor()
    user_id = id_users()
    indicator = 'requests_main_services_count'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE login = %s AND date >= %s AND service_id in (%s, %s, %s, %s)",
                    (login, first_day_of_current_period, service_id_wink, service_id_internet, service_id_telephone, service_id_iptv,))
    result_sql_requests = cur.fetchone()

    body = {
            "indicator_acronim": indicator,
            "object_id": user_id,
            "object_type": object_type,
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
    assert requestdict['data'][indicator]['value'] == result_sql_requests[0], "Результат в виджете НЕ соответствует запросу из БД"


# Виджет заявки ШПД-ТВ, тест на соответствие планового показателя по заявкам
def test_main_requests_plan_current_month():
    cur = conn.cursor()
    user_id = id_users()
    id_indicator = 110
    indicator = 'requests_main_services_count_plan'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT coalesce(value) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator, login, first_period,))
    result_sql_requests = cur.fetchone()
    if result_sql_requests is None:
        result_sql_requests = 0
    else:
        result_sql_requests = int(result_sql_requests[0])

    body = {
            "indicator_acronim": indicator,
            "object_id": user_id,
            "object_type": object_type,
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


# Виджет заявки ШПД-ТВ, тест на соответствие процента выполнения
def test_main_requests_percent_current_month():
    cur = conn.cursor()
    user_id = id_users()
    id_indicator = 110
    indicator = 'requests_main_services_count_plan_percent'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE login = %s AND date >= %s AND service_id in (%s, %s, %s, %s)",
                    (login, first_day_of_current_period, service_id_wink, service_id_internet,
                     service_id_telephone, service_id_iptv,))
    result_sql_requests_fact = int(cur.fetchone()[0])

    cur.execute("SELECT coalesce(value) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator, login, first_period,))
    result_sql_requests_plan = cur.fetchone()
    if result_sql_requests_plan is None:
        result_sql_requests_plan = 0
    else:
        result_sql_requests_plan = int(result_sql_requests_plan[0])

    if result_sql_requests_plan == 0:
        result_percent_sql = 0
    else:
        result_percent_sql = round(result_sql_requests_fact * 100 / result_sql_requests_plan, 5)

    body = {
            "indicator_acronim": indicator,
            "object_id": user_id,
            "object_type": object_type,
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
    result_percent_method = round(requestdict['data'][indicator]['value'], 5)

    assert result_percent_method == result_percent_sql, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет заявки ВН-УД, тест на соответствие фактического показателя по заявкам
def test_additional_requests_fact_current_month():
    cur = conn.cursor()
    user_id = id_users()
    indicator = 'requests_extra_services_count'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE login = %s AND date >= %s AND service_id in "
                "(%s, %s, %s)",
                    (login, first_day_of_current_period, service_id_mobile,
                     service_id_internal_camera, service_id_external_camera,))
    result_sql_requests = cur.fetchone()

    body = {
            "indicator_acronim": indicator,
            "object_id": user_id,
            "object_type": object_type,
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
    assert requestdict['data'][indicator]['value'] == result_sql_requests[0], "Результат в виджете НЕ соответствует запросу из БД"


# Виджет заявки ВН-УД, тест на соответствие планового показателя по заявкам
def test_additional_requests_plan_current_month():
    cur = conn.cursor()
    user_id = id_users()
    id_indicator = 112
    indicator = 'requests_extra_services_count_plan'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT coalesce(value) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator, login, first_period,))
    result_sql_requests = cur.fetchone()
    if result_sql_requests is None:
        result_sql_requests = 0
    else:
        result_sql_requests = int(result_sql_requests[0])

    body = {
            "indicator_acronim": indicator,
            "object_id": user_id,
            "object_type": object_type,
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


# Виджет заявки ВН УД, тест на соответствие процента выполнения
def test_additional_requests_percent_current_month():
    cur = conn.cursor()
    user_id = id_users()
    id_indicator = 112
    indicator = 'requests_extra_services_count_plan_percent'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE login = %s AND date >= %s AND service_id in "
                "(%s, %s, %s)",
                    (login, first_day_of_current_period, service_id_mobile,
                     service_id_internal_camera, service_id_external_camera,))
    result_sql_requests_fact = int(cur.fetchone()[0])

    cur.execute("SELECT coalesce(value) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator, login, first_period,))
    result_sql_requests_plan = cur.fetchone()
    if result_sql_requests_plan is None:
        result_sql_requests_plan = 0
    else:
        result_sql_requests_plan = int(result_sql_requests_plan[0])

    if result_sql_requests_plan == 0:
        result_percent = 0
    else:
        result_percent = round(result_sql_requests_fact * 100 / result_sql_requests_plan, 5)

    body = {
            "indicator_acronim": indicator,
            "object_id": user_id,
            "object_type": object_type,
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
    assert result_method == result_percent, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет подключенные услуги ШПД-ТВ, тест на соответствие фактического показателя по заявкам
def test_main_connected_services_fact_current_month():
    cur = conn.cursor()
    user_id = id_users()
    indicator = 'connected_main_services_count'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE login = %s AND date >= %s "
                "AND service_id in (%s, %s, %s, %s)",
                (login, first_day_of_current_period,
                 service_id_iptv, service_id_telephone, service_id_wink, service_id_internet,))
    result_sql_requests = cur.fetchone()

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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
    assert requestdict['data'][indicator]['value'] == result_sql_requests[
        0], "Результат в виджете НЕ соответствует запросу из БД"


# Виджет подключенные услуги ШПД-ТВ, тест на соответствие планового показателя по заявкам
def test_main_connected_services_plan_current_month():
    cur = conn.cursor()
    user_id = id_users()
    id_indicator = 111
    indicator = 'connected_main_services_count_plan'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT coalesce(value) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator, login, first_period,))
    result_sql_requests = cur.fetchone()
    if result_sql_requests is None:
        result_sql_requests = 0
    else:
        result_sql_requests = int(result_sql_requests[0])

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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
    assert requestdict['data'][indicator][
               'value'] == result_sql_requests, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет подключенные услуги ШПД-ТВ, тест на соответствие процента выполнения
def test_main_connected_services_percent_current_month():
    cur = conn.cursor()
    user_id = id_users()
    id_indicator = 111
    indicator = 'connected_main_services_count_plan_percent'
    service_name_internet = 'Домашний интернет'
    service_name_itv = 'Интерактивное ТВ'
    service_name_wink = 'Wink-ТВ-онлайн'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE login = %s AND date >= %s "
                "AND service_id in (%s, %s, %s, %s)",
                (login, first_day_of_current_period,
                 service_id_iptv, service_id_telephone, service_id_wink, service_id_internet,))
    result_sql_requests_fact = int(cur.fetchone()[0])

    cur.execute("SELECT coalesce(value) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator, login, first_period,))
    result_sql_requests_plan = cur.fetchone()
    if result_sql_requests_plan is None:
        result_sql_requests_plan = 0
    else:
        result_sql_requests_plan = int(result_sql_requests_plan[0])

    if result_sql_requests_plan == 0:
        result_sql_percent = 0
    else:
        result_sql_percent = round(result_sql_requests_fact * 100 / result_sql_requests_plan, 5)

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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
    result_method_value = round(requestdict['data'][indicator]['value'], 5)
    assert result_method_value == result_sql_percent, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет заявки ВН-УД, тест на соответствие фактического показателя по заявкам
def test_additional_connected_services_fact_current_month():
    cur = conn.cursor()
    user_id = id_users()
    indicator = 'connected_extra_services_count'
    service_name_internet = 'Домашний интернет'
    service_name_itv = 'Интерактивное ТВ'
    service_name_wink = 'Wink-ТВ-онлайн'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE login = %s AND date >= %s "
                "AND NOT service_name in (%s, %s, %s)",
                    (login, first_day_of_current_period, service_name_internet, service_name_itv, service_name_wink,))
    result_sql_requests = cur.fetchone()

    body = {
            "indicator_acronim": indicator,
            "object_id": user_id,
            "object_type": object_type,
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
    assert requestdict['data'][indicator]['value'] == result_sql_requests[0], "Результат в виджете НЕ соответствует запросу из БД"


# Виджет подключенные услуги ВН-УД, тест на соответствие планового показателя по заявкам
def test_additional_connected_services_plan_current_month():
    cur = conn.cursor()
    user_id = id_users()
    id_indicator = 113
    indicator = 'connected_extra_services_count_plan'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT coalesce(value) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator, login, first_period,))
    result_sql_requests = cur.fetchone()
    if result_sql_requests is None:
        result_sql_requests = 0
    else:
        result_sql_requests = int(result_sql_requests[0])

    body = {
            "indicator_acronim": indicator,
            "object_id": user_id,
            "object_type": object_type,
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


# Виджет подключенные услуги ВН-УН, тест на соответствие процента выполнения
def test_additional_connected_services_percent_current_month():
    cur = conn.cursor()
    user_id = id_users()
    id_indicator = 113
    indicator = 'connected_extra_services_count_plan_percent'
    service_name_internet = 'Домашний интернет'
    service_name_itv = 'Интерактивное ТВ'
    service_name_wink = 'Wink-ТВ-онлайн'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE login = %s AND date >= %s "
                "AND NOT service_name in (%s, %s, %s)",
                    (login, first_day_of_current_period, service_name_internet, service_name_itv, service_name_wink,))
    result_sql_requests_fact = int(cur.fetchone()[0])

    cur.execute("SELECT coalesce(value) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator, login, first_period,))
    result_sql_requests_plan = cur.fetchone()
    if result_sql_requests_plan is None:
        result_sql_requests_plan = 0
    else:
        result_sql_requests_plan = int(result_sql_requests_plan[0])

    if result_sql_requests_plan == 0:
        result_percent = 0
    else:
        result_percent = result_sql_requests_fact * 100 / result_sql_requests_plan

    body = {
            "indicator_acronim": indicator,
            "object_id": user_id,
            "object_type": object_type,
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

    assert requestdict['data'][indicator]['value'] == result_percent, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет рабочие часы, тест на соответствие фактического показателя
def test_work_hours_fact_current_month():
    cur = connection.cursor()
    user_id = id_users()
    indicator = 'work_hours'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT SUM(duration) FROM mv_user_online_time_daily WHERE login = %s AND date_work >= %s AND status = 'online'",
                (login, first_day_of_current_period,))
    result_sql_requests = cur.fetchone()

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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
    assert requestdict['data'][indicator]['value'] == result_sql_requests[
        0], "Результат в виджете НЕ соответствует запросу из БД"


# Виджет рабочие часы, тест на соответствие планового показателя
def test_work_hours_plan_current_month():
    cur = conn.cursor()
    user_id = id_users()
    id_indicator = 85
    indicator = 'work_hours_plan_new'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT coalesce(value) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator, login, first_period,))
    result_sql_requests = cur.fetchone()
    if result_sql_requests is None:
        result_sql_requests = 0
    else:
        result_sql_requests = int(result_sql_requests[0])

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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
    assert requestdict['data'][indicator][
               'value'] == result_sql_requests, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет рабочие часы, тест на соответствие процента выполнения
def test_work_hours_percent_current_month():
    cur = conn.cursor()
    cur_naumen = connection.cursor()
    user_id = id_users()
    id_indicator = 85
    indicator = 'work_hours_plan_percent_new'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur_naumen.execute("SELECT SUM(duration) FROM mv_user_online_time_daily WHERE login = %s AND date_work >= %s AND "
                       "status = 'online'", (login, first_day_of_current_period,))
    result_sql_requests_fact = int(cur_naumen.fetchone()[0])

    cur.execute("SELECT coalesce(value) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator, login, first_period,))
    result_sql_requests_plan = cur.fetchone()
    if result_sql_requests_plan is None:
        result_sql_requests_plan = 0
    else:
        result_sql_requests_plan = int(result_sql_requests_plan[0])

    if result_sql_requests_plan == 0:
        result_percent = 0
    else:
        result_percent = result_sql_requests_fact * 100 / result_sql_requests_plan

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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
    result_to_method = requestdict['data'][indicator]['value']
    result_to_method = round(result_to_method, 5)
    result_percent = round(result_percent, 5)

    assert result_to_method == result_percent, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет Конверсия из заявок в подключенные услуги, тест на соответствие факта
def test_requests_connection_conversion_current_month():
    cur = conn.cursor()
    user_id = id_users()
    service_id_internet = 37
    service_id_iptv = 34
    service_id_wink = 43
    indicator = 'requests_connected_conversion'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE login = %s AND date >= %s "
                "AND service_id IN (%s, %s, %s)",
                    (login, first_day_of_current_period, service_id_iptv, service_id_internet, service_id_wink,))
    result_sql_requests_fact = int(cur.fetchone()[0])

    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE login = %s AND date >= %s "
                "and service_id in (%s, %s, %s)",
                (login, first_day_of_current_period, service_id_iptv, service_id_internet, service_id_wink,))
    result_sql_connected_services_fact = cur.fetchone()[0]

    if result_sql_connected_services_fact == 0:
        result_sql_conversion = 0
    else:
        result_sql_conversion = round(result_sql_connected_services_fact * 100 / result_sql_requests_fact, 5)

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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

    result_method_conversion = round((requestdict['data'][indicator]['value']), 5)

    assert result_method_conversion == result_sql_conversion, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет Конверсия из заявок в подключенные услуги, тест на соответствие факта
def test_kpi_requests_connection_conversion_current_month():
    cur = conn.cursor()
    user_id = id_users()
    indicator = 'kpi_requests_connected_conversion_new'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE login = %s AND date >= %s and service_id in (%s, %s, %s, %s)",
                    (login, first_day_of_current_period, service_id_iptv, service_id_internet, service_id_wink,
                     service_id_telephone,))
    result_sql_requests_fact = int(cur.fetchone()[0])

    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE login = %s AND date >= %s and service_id in "
                "(%s, %s, %s, %s)",
                    (login, first_day_of_current_period, service_id_iptv, service_id_internet, service_id_wink,
                     service_id_telephone,))
    result_sql_connected_services_fact = cur.fetchone()[0]

    if result_sql_connected_services_fact == 0:
        result_sql_conversion = 0
    else:
        result_sql_conversion = result_sql_connected_services_fact * 100 / result_sql_requests_fact

    if result_sql_conversion < 40:
        kpi = 0
    elif result_sql_conversion < 50:
        kpi = 0.6
    elif result_sql_conversion < 60:
        kpi = 0.8
    elif result_sql_conversion < 70:
        kpi = 1
    else:
        kpi = 1.2

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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

    assert requestdict['data']['kpi_requests_connected_conversion_new'][
                'value'] == kpi, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет qm факт
def test_qm_fact_current_month():
    cur = conn.cursor()
    user_id = id_users()
    indicator = 'operator_rate'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT value FROM operator_rate WHERE operator_id = %s AND day >= %s ORDER BY day DESC",
                    (user_id, first_day_of_current_period,))
    result_sql_requests_qm = cur.fetchone()
    if result_sql_requests_qm is not None:
        result_sql_requests_qm = int(result_sql_requests_qm[0])

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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

    assert requestdict['data'][indicator][
               'value'] == result_sql_requests_qm, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет рабочие часы, тест на соответствие процента выполнения
def test_kpi_qm_fact_current_month():
    cur = conn.cursor()
    user_id = id_users()
    indicator = 'operator_rate_coefficient_new'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT value FROM operator_rate WHERE operator_id = %s AND day >= %s",
                    (user_id, first_day_of_current_period,))
    result_sql_operator_rate = cur.fetchone()
    if result_sql_operator_rate is not None:
        result_sql_operator_rate = int(result_sql_operator_rate[0])
    else:
        result_sql_operator_rate = 0

    if result_sql_operator_rate < 30:
        kpi = 0
    elif result_sql_operator_rate < 50:
        kpi = 0.4
    elif result_sql_operator_rate < 60:
        kpi = 0.6
    elif result_sql_operator_rate < 75:
        kpi = 0.7
    else:
        kpi = 1

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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

    assert requestdict['data'][indicator][
               'value'] == kpi, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет прогноз дохода - норма занятости
def test_employment_to_work_hours_current_month():
    cur_naumen = connection.cursor()
    user_id = id_users()
    indicator = 'rostelecom_employment_detail_work_spedometer'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    # cur_naumen.execute(
    #     "select sum(duration) from mv_user_status_full_daily where login = %s and date_work >= %s and "
    #     "(away_status_reason != 'CustomAwayReason2' and away_status_reason != 'CustomAwayReason3' and "
    #     "away_status_reason != 'PrepareToWork' and away_status_reason != 'initializing' or away_status_reason is null) "
    #     "and main_status != 'offline' and (wrapup != 'wrapup' or wrapup is null or away_status_reason is not null) and "
    #     "(away_status != 'away' or away_status is null or away_status_reason is not null) and (wrapup <> 'wrapup' or "
    #     "wrapup is null or away_status_reason <> 'CustomAwayReason5' and away_status_reason <> 'CustomAwayReason7' and "
    #     "away_status_reason <> 'CustomAwayReason4')",
    #     (login, first_period,))

    cur_naumen.execute(
        "select sum(duration) from mv_user_status_full_daily where login = %s and date_work >= %s and"
        " (normal in ('normal') or ringing in ('ringing') or speaking in ('speaking') or "
        "away_status_reason in ('CustomAwayReason4', 'CustomAwayReason5', 'CustomAwayReason6', 'CustomAwayReason7')) and  "
        "( away_status_reason = 'CustomAwayReason6' or wrapup is null)",
        (login, first_period,))
    result_employment = int(cur_naumen.fetchone()[0])

    cur_naumen.execute("select sum(duration) from mv_user_online_time_daily where login = %s and date_work >= %s and "
                       "status = 'online'", (login, first_period,))
    # online_time = cur_naumen.fetchone()[0]
    #
    # cur_naumen.execute("select sum(duration) from mv_user_status_full_daily where login = %s and date_work >= %s and "
    #                    "(normal in ('normal') or ringing in ('ringing') or speaking in ('speaking') or "
    #                     "away_status_reason in ('CustomAwayReason1', 'CustomAwayReason2', 'CustomAwayReason3',"
    #                    "'CustomAwayReason4', 'CustomAwayReason5', 'CustomAwayReason6', 'CustomAwayReason7') or  "
    #                     " wrapup in ('wrapup') or away_status in ('away'))", (login, first_period,))
    # online_lime_new = cur_naumen.fetchone()[0]
    # print(online_time, online_lime_new)

    result_online_time = int(cur_naumen.fetchone()[0])
    result_employment = round(result_employment * 100 / result_online_time, 2)

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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
    assert requestdict['data'][indicator][
               'value'] == result_employment, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет прогноз дохода - подключенные услуги
def test_connected_services_current_month():
    cur = conn.cursor()
    user_id = id_users()
    indicator = 'connected_all_services_count'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("select count(id) from rtk_connected_services where user_id = %s and date >= %s",
                (user_id, first_period,))
    result_count_services = int(cur.fetchone()[0])

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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
    assert requestdict['data'][indicator][
               'value'] == result_count_services, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет прогноз дохода - норма по часам
def test_normal_to_work_hours_current_month():
    cur_naumen = connection.cursor()
    cur = conn.cursor()
    user_id = id_users()
    indicator = 'work_hours_plan_percent_new'
    id_indicator = 85

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur_naumen.execute("select sum(duration) from mv_user_online_time_daily where login = %s and date_work >= %s and "
                       "status = 'online'", (login, first_period,))
    result_online_time_by_operator = float(cur_naumen.fetchone()[0]/3600)

    cur.execute(
        "SELECT coalesce(value) FROM plan_objects_indicators JOIN objects_indicators ON "
        "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
        "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
        (id_indicator, login, first_period,))
    result_plat_time_by_operator = cur.fetchone()
    if result_plat_time_by_operator is None:
        result_plat_time_by_operator = 0
    else:
        result_plat_time_by_operator = float(result_plat_time_by_operator[0])

    if result_plat_time_by_operator == 0:
        result_norm_to_hours_sql = 0
    else:
        result_norm_to_hours_sql = round(result_online_time_by_operator / (result_plat_time_by_operator / 3600) * 100, 5)

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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
    result_norm_to_hours_method = round(requestdict['data'][indicator]['value'], 5)
    assert result_norm_to_hours_method == result_norm_to_hours_sql, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет занятость - простой
def test_widget_employment_main_idle_current_month():
    cur_naumen = connection.cursor()
    user_id = id_users()
    indicator = 'rostelecom_employment_detail_idle'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur_naumen.execute("select sum(duration) from mv_user_online_time_daily where login = %s and date_work >= %s and "
                       "status = 'online'", (login, first_period,))
    result_online_time_by_operator = float(cur_naumen.fetchone()[0])

    cur_naumen.execute(
        "select sum(duration) from mv_user_status_full_daily where "
        "date_work >= %s and login = %s and "
        "(away_status_reason in ('CustomAwayReason2', 'PrepareToWork') or "
        "away_status_reason is null or (wrapup = 'wrapup' and away_status_reason != 'CustomAwayReason6')) "
        "and (normal != 'normal' or normal is null) and (ringing != 'ringing' or ringing is null) and "
        "(speaking != 'speaking' or speaking is null) and main_status != 'offline'",
        (first_period, login,))
    result_idle_time_by_operator = float(cur_naumen.fetchone()[0])

    result_idle_in_employment = round(result_idle_time_by_operator * 100 / result_online_time_by_operator, 2)

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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
    assert requestdict['data'][indicator][
               'value']['value'] == result_idle_in_employment, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет занятость - отдых
def test_widget_employment_main_away_current_month():
    cur_naumen = connection.cursor()
    user_id = id_users()
    indicator = 'rostelecom_employment_detail_away'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur_naumen.execute("select sum(duration) from mv_user_online_time_daily where login = %s and date_work >= %s and "
                       "status = 'online'", (login, first_period,))
    result_online_time_by_operator = float(cur_naumen.fetchone()[0]/3600)

    cur_naumen.execute(
            "select sum(duration) from mv_user_status_full_daily where login = %s and date_work >= %s and "
            "(away_status_reason in ('CustomAwayReason1', 'CustomAwayReason3') and "
            "(normal is null and ringing is null and speaking is null and wrapup is null ))",
            (login, first_period,))
    result_away_time_by_operator = float(cur_naumen.fetchone()[0])

    result_away_in_employment = round(result_away_time_by_operator / 3600 * 100 / result_online_time_by_operator, 2)

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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
    assert requestdict['data'][indicator][
               'value']['value'] == result_away_in_employment, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет ВСЕ ЗАЯВКИ шпд итв вн уд ФАКТ
def test_widget_all_requests_fact_current_month():
    cur = conn.cursor()
    user_id = id_users()
    indicator = 'requests_created'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE login = %s AND date >= %s AND"
                " service_id IN (%s, %s, %s, %s)",
                        (login, first_day_of_current_period,
                         service_id_iptv, service_id_internet, service_id_wink, service_id_telephone,))
    result_all_requests = cur.fetchone()

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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
    assert requestdict['data'][indicator][
               'value'] == result_all_requests[0], "Результат в виджете НЕ соответствует запросу из БД"


# Виджет ВСЕ ЗАЯВКИ шпд итв вн уд ПЛАН
def test_widget_all_requests_plan_current_month():
    cur = conn.cursor()
    user_id = id_users()
    indicator = 'requests_created_plan'
    id_indicator = 108

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT coalesce(value) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                    (id_indicator, login, first_period,))
    result_all_requests_to_plan = cur.fetchone()
    if result_all_requests_to_plan is None:
        result_all_requests_to_plan = 0
    else:
        result_all_requests_to_plan = float(result_all_requests_to_plan[0])

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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
    assert requestdict['data'][indicator][
               'value'] == result_all_requests_to_plan, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет ВСЕ ЗАЯВКИ шпд итв вн уд ПРОЦЕНТ
def test_widget_all_requests_percent_current_month():
    cur = conn.cursor()
    user_id = id_users()
    indicator = 'requests_created_plan_percent'
    id_indicator = 108
    service_name_guarantee = 'Гарантия'
    service_name_guarantee_plus = 'Гарантия+'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    cur.execute("SELECT coalesce(value) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                    (id_indicator, login, first_period,))
    result_all_requests_to_plan = cur.fetchone()
    if result_all_requests_to_plan is None:
        result_all_requests_to_plan = 0
    else:
        result_all_requests_to_plan = float(result_all_requests_to_plan[0])

    cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE login = %s AND date >= %s AND"
                " service_id IN (%s, %s, %s, %s)",
                        (login, first_day_of_current_period,
                         service_id_iptv, service_id_internet, service_id_wink, service_id_telephone,))
    result_all_requests_fact = float(cur.fetchone()[0])

    if result_all_requests_to_plan == 0:
        result_sql_percent = 0
    else:
        result_sql_percent = round(result_all_requests_fact * 100 / result_all_requests_to_plan, 5)

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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
    result_method_value = round(requestdict['data'][indicator]['value'], 5)
    assert result_method_value == result_sql_percent, "Результат в виджете НЕ соответствует запросу из БД"


# Виджет ВСЕ ЗАЯВКИ ДИНАМИКА ЗАЯВОК В ЧАС
def test_widget_all_requests_dynamic_last_week_current_month():
    cur = conn.cursor()
    cur_naumen = connection.cursor()
    user_id = id_users()
    indicator = 'requests_per_hour_last_week'
    service_name_guarantee = 'Гарантия'
    service_name_guarantee_plus = 'Гарантия+'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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

    array_of_mondays = []
    date_now = datetime.date.today()
    first_day_of_current_month = date_current.replace(day=1)

    while True:
        if datetime.datetime.weekday(first_day_of_current_month) == 0:
            first_day_in_string_type = str(first_day_of_current_month)
            array_of_mondays.append(first_day_in_string_type)
        first_day_of_current_month = first_day_of_current_month + datetime.timedelta(days=1)
        if first_day_of_current_month >= date_now:
            break
    count_week_with_kpi = len(array_of_mondays)
    array_method_mondays = []
    array_method_values = []
    for key in range(1, count_week_with_kpi+1):
        array_method_mondays.append(requestdict["data"][indicator]['value']['mondays'][key]['title'])
        array_method_values.append(requestdict["data"][indicator]['value']['mondays'][key]['value'])
        if key > count_week_with_kpi:
            break

    # Вычисление и сравнение KPI за текущий месяц
    cur_naumen.execute("SELECT SUM(duration)/3600 FROM mv_user_online_time_daily WHERE login = %s AND date_work >= %s "
                       "AND status = 'online'", (login, first_period,))
    online_time_full_current_period = cur_naumen.fetchone()[0]

    cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE user_id = %s AND date >= %s "
                "AND service_id IN (%s, %s, %s, %s, %s, %s, %s)",
                (user_id, first_period, service_id_telephone, service_id_wink, service_id_internet,
                 service_id_iptv, service_id_external_camera, service_id_internal_camera, service_id_mobile))
    count_requests_full_current_period = cur.fetchone()[0]

    kpi_full_current_period_sql = float(round(count_requests_full_current_period / online_time_full_current_period, 2))
    kpi_full_current_period_method = requestdict["data"][indicator]['value']['month']['value']
    assert kpi_full_current_period_sql == kpi_full_current_period_method

    # Вычисление и сравнение KPI за первый дебильный период
    if count_week_with_kpi >= 1:
        kpi_first_week_in_method = array_method_values[0]
        last_day_of_first_period = datetime.datetime.strptime(array_of_mondays[0], '%Y-%m-%d').date() - datetime.timedelta(days=1)
        cur_naumen.execute(
            "SELECT SUM(duration)/3600 FROM mv_user_online_time_daily WHERE login = %s AND date_work between %s and %s "
            "AND status = 'online'", (login, first_period, last_day_of_first_period,))
        online_time_first_period = cur_naumen.fetchone()[0]

        cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE user_id = %s AND date between %s and %s "
                    "AND service_id IN (%s, %s, %s, %s, %s, %s, %s)",
                    (user_id, first_period, last_day_of_first_period, service_id_telephone, service_id_wink, service_id_internet,
                     service_id_iptv, service_id_external_camera, service_id_internal_camera, service_id_mobile))

        count_requests_first_period = cur.fetchone()[0]
        if online_time_first_period is None:
            kpi_first_week_sql = 0
        else:
            kpi_first_week_sql = float(round(count_requests_first_period / online_time_first_period, 2))
        assert kpi_first_week_in_method == kpi_first_week_sql

    # Вычисление за второй и последующий периоды
    for i in range(1, count_week_with_kpi):
        kpi_next_week_in_method = array_method_values[i]
        last_day_of_next_period = datetime.datetime.strptime(array_of_mondays[i], '%Y-%m-%d').date() - datetime.timedelta(days=1)

        cur_naumen.execute(
            "SELECT SUM(duration)/3600 FROM mv_user_online_time_daily WHERE login = %s AND date_work between %s and %s "
            "AND status = 'online'", (login, first_period, last_day_of_next_period,))
        online_time_next_period = cur_naumen.fetchone()[0]

        cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE user_id = %s AND date between %s and %s "
                    "AND service_id IN (%s, %s, %s, %s, %s, %s, %s)",
                    (user_id, first_period, last_day_of_next_period, service_id_telephone, service_id_wink, service_id_internet,
                     service_id_iptv, service_id_external_camera, service_id_internal_camera, service_id_mobile))
        count_requests_next_period = cur.fetchone()[0]
        kpi_next_week_sql = float(round(count_requests_next_period / online_time_next_period, 2))
        assert kpi_next_week_in_method == kpi_next_week_sql


# Виджет ЭФФЕКТИВНОСТЬ, среднее время оформления заявки
def test_widget_efficiency_average_processing_time_by_operator_current_month():
    cur = conn.cursor()
    cur_naumen = connection.cursor()
    user_id = id_users()
    indicator = 'average_processing_time_by_operator'
    service_name_guarantee = 'Гарантия'
    service_name_guarantee_plus = 'Гарантия+'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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

    # Нахождение в статусе CustomAwayReason6
    cur_naumen.execute("SELECT SUM(duration)/60 FROM mv_user_status_full_daily WHERE login = %s AND date_work >= %s "
                       "AND away_status_reason = 'CustomAwayReason6'",
                       (login, first_period,))
    time_in_status_custom_away_reason6 = cur_naumen.fetchone()[0]

    cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE user_id = %s AND date >= %s "
                "AND service_id IN (%s, %s, %s, %s, %s, %s, %s)",
                (user_id, first_period, service_id_telephone, service_id_wink, service_id_internet,
                 service_id_iptv, service_id_external_camera, service_id_internal_camera, service_id_mobile))
    count_requests_full_current_period = cur.fetchone()[0]

    if count_requests_full_current_period == 0:
        average_time_to_requests_sql = 0
    else:
        average_time_to_requests_sql = \
            float(round(time_in_status_custom_away_reason6 / count_requests_full_current_period, 5))

    average_time_to_requests_method = round(requestdict['data'][indicator]['value'], 5)
    assert average_time_to_requests_sql == average_time_to_requests_method


# Виджет ЭФФЕКТИВНОСТЬ, среднее время оформления заявки
def test_widget_efficiency_requests_in_hour_by_operator_current_month():
    cur = conn.cursor()
    cur_naumen = connection.cursor()
    user_id = id_users()
    indicator = 'requests_per_hour_by_operator'
    service_name_guarantee = 'Гарантия'
    service_name_guarantee_plus = 'Гарантия+'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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

    cur_naumen.execute("SELECT SUM(duration) FROM mv_user_online_time_daily WHERE login = %s AND date_work >= %s "
                       "AND status = 'online'", (login, first_period,))
    online_time_by_operator_in_current_period = cur_naumen.fetchone()[0] / 3600

    cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE user_id = %s AND date >= %s "
                "AND service_id IN (%s, %s, %s, %s, %s, %s, %s)",
                (user_id, first_period, service_id_telephone, service_id_wink, service_id_internet,
                 service_id_iptv, service_id_external_camera, service_id_internal_camera, service_id_mobile))
    count_requests_full_current_period = cur.fetchone()[0]

    efficiency_request_in_hour_sql = float(round(count_requests_full_current_period /
                                                 online_time_by_operator_in_current_period, 5))

    efficiency_request_in_hour_method = round(requestdict['data'][indicator]['value'], 5)
    assert efficiency_request_in_hour_sql == efficiency_request_in_hour_method


# Виджет ЭФФЕКТИВНОСТЬ, работа с возражениями
def test_widget_efficiency_work_to_objections_by_operator_current_month():
    cur = conn.cursor()
    user_id = id_users()
    indicator = 'operator_objections_rate'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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

    cur.execute("SELECT objections_rate FROM operator_rate WHERE operator_id = %s AND day >= %s  ORDER BY day DESC",
                (user_id, first_period,))
    objections_rate_sql = cur.fetchone()
    if objections_rate_sql is not None:
        objections_rate_sql = objections_rate_sql[0]
    else:
        objections_rate_sql = 0
    objections_rate_method = requestdict['data'][indicator]['value']
    assert objections_rate_sql == objections_rate_method


# Виджет ВСЕ ПОДКЛЮЧЕННЫЕ УСЛУГИ, факт
def test_widget_all_connected_services_fact_operator_current_month():
    cur = conn.cursor()
    user_id = id_users()
    indicator = 'final_services_connected_all'

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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

    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE user_id = %s AND date >= %s",
                (user_id, first_period,))
    all_connected_services_fact_sql = cur.fetchone()[0]
    all_connected_services_fact_method = requestdict['data'][indicator]['value']
    assert all_connected_services_fact_sql == all_connected_services_fact_method


# Виджет ВСЕ ПОДКЛЮЧЕННЫЕ УСЛУГИ, план
def test_widget_all_connected_services_plan_operator_current_month():
    cur = conn.cursor()
    user_id = id_users()
    indicator = 'connected_all_services_count_plan'
    id_indicator = 103

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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

    cur.execute("SELECT coalesce(value) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator, login, first_period,))
    all_connected_services_plan_sql = cur.fetchone()
    if all_connected_services_plan_sql is None:
        all_connected_services_plan_sql = 0
    else:
        all_connected_services_plan_sql = int(all_connected_services_plan_sql[0])
    all_connected_services_plan_method = requestdict['data'][indicator]['value']
    assert all_connected_services_plan_sql == all_connected_services_plan_method


# Виджет ВСЕ ПОДКЛЮЧЕННЫЕ УСЛУГИ, процент выполнения плана
def test_widget_all_connected_services_percent_operator_current_month():
    cur = conn.cursor()
    user_id = id_users()
    indicator = 'connected_all_services_count_plan_percent'
    id_indicator = 103

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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

    cur.execute("SELECT coalesce(value) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator, login, first_period,))
    all_connected_services_plan_sql = cur.fetchone()
    if all_connected_services_plan_sql is None:
        all_connected_services_plan_sql = 0
    else:
        all_connected_services_plan_sql = int(all_connected_services_plan_sql[0])

    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE user_id = %s AND date >= %s",
                (user_id, first_period,))
    all_connected_services_fact_sql = cur.fetchone()[0]

    if all_connected_services_plan_sql == 0:
        all_connected_services_percent_sql = 0
    else:
        all_connected_services_percent_sql = round(all_connected_services_fact_sql * 100 / all_connected_services_plan_sql, 5)
    all_connected_services_percent_method = round(requestdict['data'][indicator]['value'], 5)
    assert all_connected_services_percent_sql == all_connected_services_percent_method


# Виджет прогноз дохода с деньгами, ПРОГНОЗ ДОХОДА ТЕКУЩИЙ ДЕНЬ
def test_widget_income_forecast_on_current_day():
    cur = conn.cursor()
    cur_naumen = connection.cursor()
    user_id = id_users()
    indicator = 'rtk_payment_detailed'
    id_indicator = 85  # id показателя для получения плана по рабочим часам
    id_indicator_connected_services_main = 111
    id_indicator_connected_services_slave = 113
    rate_the_bablo = rate_of_the_positions()

    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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

    income_forecast_method = requestdict['data'][indicator]['value']  # Результат вычислений с бэкенда

    cur_naumen.execute("SELECT SUM(duration) FROM mv_user_online_time_daily WHERE login = %s AND date_work >= %s AND "
                       "status = 'online'",
                       (login, first_day_of_current_period,))
    online_time_current = round(float(cur_naumen.fetchone()[0] / 3600), 0)  # Текущее время в онлайне

    # Получение плана по рабочим часам
    cur.execute("SELECT coalesce(value::float, 0) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator, login, first_period,))
    online_time_plan = cur.fetchone()
    if online_time_plan is None:
        online_time_plan = 0
    else:
        online_time_plan = float(online_time_plan[0]) / 3600

    # Вычисление коэффициента по отработанным часам в соответствии с отработанным временем
    if online_time_plan == 0:
        forecast_online_time_without_kpi_percent_to_plan = 0
    else:
        forecast_online_time_without_kpi_percent_to_plan = online_time_current * 100 / online_time_plan
    if forecast_online_time_without_kpi_percent_to_plan < 60:
        kpi_online_time = 0
    elif forecast_online_time_without_kpi_percent_to_plan < 80:
        kpi_online_time = 0.5
    elif forecast_online_time_without_kpi_percent_to_plan < 95:
        kpi_online_time = 0.8
    else:
        kpi_online_time = 1

    # Получение нормы занятости
    cur_naumen.execute(
        "select sum(duration) from mv_user_status_full_daily where login = %s and date_work >= %s and "
        "(away_status_reason not in ('CustomAwayReason2', 'CustomAwayReason3', 'PrepareToWork', 'initializing')"
        " or away_status_reason is null) and main_status != 'offline' and (wrapup != 'wrapup' or wrapup is null or away_status_reason is not null) and "
        "(away_status != 'away' or away_status is null or away_status_reason is not null) and (wrapup <> 'wrapup' or "
        "wrapup is null or away_status_reason not in ('CustomAwayReason5', 'CustomAwayReason7', 'CustomAwayReason4'))",
        (login, first_period,))
    result_employment = round(int(cur_naumen.fetchone()[0] / 3600) * 100 / online_time_current, 2)  # норма занятости

    # Вычисление коэффициента по выполнению нормы занятости:
    if result_employment < 50:
        kpi_employment = 0
    elif result_employment < 70:
        kpi_employment = 0.4
    elif result_employment < 85:
        kpi_employment = 0.7
    elif result_employment < 91:
        kpi_employment = 1
    else:
        kpi_employment = 1.2

    # ОПЛАТА ЗА ЧАСЫ ИТОГ: количество отработанных часов * ставка за рабочие часы * кэф за норму занятости * кэф за отработанные часы
    payment_to_hours = online_time_current * rate_the_bablo * kpi_online_time * kpi_employment

    # ОПЛАТА ЗА ОСНОВНЫЕ УСЛУГИ
    # Вычисление КЭФ премиального заработка по основным услугам
    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE user_id = %s AND date >= %s and "
                "service_id in (%s, %s, %s, %s, %s, %s)",
                (user_id, first_period, service_id_iptv, service_id_internet, service_id_wink, service_id_telephone,
                 service_id_2_in_1, service_id_3_in_1,))
    current_connected_main_services = cur.fetchone()[0]  # Количество основных услуг сейчас

    # кэф премиального заработка
    if current_connected_main_services == 0:
        kpi_prize_main = 0
    elif current_connected_main_services < 31:
        kpi_prize_main = 1
    elif current_connected_main_services < 41:
        kpi_prize_main = 1.2
    elif current_connected_main_services < 51:
        kpi_prize_main = 1.3
    else:
        kpi_prize_main = 1.4

    # Вычисление КЭФ премиального заработка по ДОПОЛНИТЕЛЬНЫМ услугам
    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE user_id = %s AND date >= %s and "
                "service_id in (%s, %s, %s)",
                (user_id, first_period, service_id_external_camera, service_id_internal_camera, service_id_mobile,))
    current_connected_slave_services = cur.fetchone()[0]  # Количество дополнительных услуг сейчас

    # кэф премиального заработка
    if current_connected_slave_services == 0:
        kpi_prize_slave = 0
    elif current_connected_slave_services < 5:
        kpi_prize_slave = 1
    elif current_connected_slave_services == 5:
        kpi_prize_slave = 1.2
    elif current_connected_slave_services == 6:
        kpi_prize_slave = 1.3
    elif current_connected_slave_services == 7:
        kpi_prize_slave = 1.4
    else:
        kpi_prize_slave = 1.5

    ################################## КОЛИЧЕСТВО КАЖДОЙ ПОДКЛЮЧЕННОЙ УСЛУГИ ##########################################
    # Количество услуг шпд
    cur.execute(
        "SELECT COUNT(id) FROM rtk_connected_services WHERE user_id = %s AND date >= %s AND service_id = %s AND "
        "service_pack_id is null",
        (user_id, first_period, service_id_internet,))
    count_service_id_internet = cur.fetchone()
    if count_service_id_internet is not None:
        count_service_id_internet = count_service_id_internet[0]

    # Количество услуг IP-TV
    cur.execute(
        "SELECT COUNT(id) FROM rtk_connected_services WHERE user_id = %s AND date >= %s AND service_id = %s AND "
        "service_pack_id is null",
        (user_id, first_period, service_id_iptv,))
    count_service_id_iptv = cur.fetchone()
    if count_service_id_iptv is not None:
        count_service_id_iptv = count_service_id_iptv[0]

    # Количество услуг мобильная связь
    cur.execute(
        "SELECT COUNT(id) FROM rtk_connected_services WHERE user_id = %s AND date >= %s AND service_id = %s AND "
        "service_pack_id is null",
        (user_id, first_period, service_id_mobile,))
    count_service_id_mobile = cur.fetchone()
    if count_service_id_mobile is not None:
        count_service_id_mobile = count_service_id_mobile[0]

    # Количество услуг WINK
    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE user_id = %s AND date >= %s AND service_id = %s",
                (user_id, first_period, service_id_wink,))
    count_service_id_wink = cur.fetchone()
    if count_service_id_wink is not None:
        count_service_id_wink = count_service_id_wink[0]

    # Количество услуг ВНУТРЕННЯЯ КАМЕРА
    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE user_id = %s AND date >= %s AND service_id = %s AND "
                "service_pack_id is null",
                (user_id, first_period, service_id_internal_camera,))
    count_service_id_internal_camera = cur.fetchone()
    if count_service_id_internal_camera is not None:
        count_service_id_internal_camera = count_service_id_internal_camera[0]

    # Количество услуг ВНУТРЕННЯЯ КАМЕРА
    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE user_id = %s AND date >= %s AND service_id = %s",
                (user_id, first_period, service_id_external_camera,))
    count_service_id_external_camera = cur.fetchone()
    if count_service_id_external_camera is not None:
        count_service_id_external_camera = count_service_id_external_camera[0]

    # Количество услуг 2 в 1
    cur.execute(
        "SELECT COUNT(id) FROM rtk_connected_services WHERE user_id = %s AND date >= %s AND service_id in (%s, %s) AND "
        "service_pack_id = %s",
        (user_id, first_period, service_id_internet, service_id_iptv, 1,))
    count_service_id_2_in_1 = cur.fetchone()
    if count_service_id_2_in_1 is not None:
        count_service_id_2_in_1 = count_service_id_2_in_1[0] / 2

    # Количество услуг 3 в 1
    cur.execute(
        "SELECT COUNT(id) FROM rtk_connected_services WHERE user_id = %s AND date >= %s AND service_id in (%s, %s, %s) "
        "AND service_pack_id = %s",
        (user_id, first_period, service_id_internet, service_id_iptv, service_id_internal_camera, 2,))
    count_service_id_3_in_1 = cur.fetchone()
    if count_service_id_3_in_1 is not None:
        count_service_id_3_in_1 = count_service_id_3_in_1[0] / 3

    # ОПЛАТА ЗА УСЛУГИ БЕЗ ПРЕМИАЛЬНОГО КОЭФФИЦИЕНТА
    payment_to_main_connected_services = \
        (count_service_id_internet * rate_internet) + \
        (count_service_id_iptv * rate_iptv) + \
        (count_service_id_wink * rate_wink) + \
        (count_service_id_2_in_1 * rate_2_in_1) + \
        (count_service_id_3_in_1 * rate_3_in_1)

    payment_to_slave_connected_services = \
        (count_service_id_internal_camera * rate_internal_camera) + \
        (count_service_id_external_camera * rate_external_camera) + \
        (count_service_id_mobile * rate_mobile)

    # ИТОГОВАЯ ОПЛАТА ЗА ОСНОВНЫЕ УСЛУГИ:
    result_payment_main_service = payment_to_main_connected_services * kpi_prize_main
    # ИТОГОВАЯ ОПЛАТА ЗА ДОПОЛНИТЕЛЬНЫЕ УСЛУГИ:
    result_payment_slave_service = payment_to_slave_connected_services * kpi_prize_slave

    ############### Вычисление основного KPI ########################
    cur.execute("SELECT value FROM operator_rate WHERE day >= %s AND operator_id = %s ORDER BY day DESC",
                (first_period, user_id,))
    qm_value = cur.fetchone()
    if qm_value is not None:
        qm_value = qm_value[0]
    else:
        qm_value = 0

    # Вычисление KPI качества прослушанных диалогов
    if qm_value < 50:
        kpi_qm = 0
    elif qm_value < 60:
        kpi_qm = 0.4
    elif qm_value < 75:
        kpi_qm = 0.7
    else:
        kpi_qm = 1

    # Вычисление KPI плана по ОСНОВНЫМ услугам
    cur.execute("SELECT value FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator_connected_services_main, login, first_period,))
    result_main_services_to_plan = cur.fetchone()
    if result_main_services_to_plan is None:
        result_main_services_to_plan = 0
    else:
        result_main_services_to_plan = float(result_main_services_to_plan[0])

    # Вычисление процента выполнения плана по основным услугам
    if result_main_services_to_plan == 0:
        result_main_services_percent = 0
    else:
        result_main_services_percent = current_connected_main_services * 100 / result_main_services_to_plan

    if result_main_services_percent < 50:
        kpi_main_services = 0
    elif result_main_services_percent < 80:
        kpi_main_services = 0.4
    elif result_main_services_percent < 100:
        kpi_main_services = 0.7
    else:
        kpi_main_services = 1

    #  Вычисление KPI конвертации по ОСНОВНЫМ УСЛУГАМ
    cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE user_id = %s AND date >= %s"
                " and service_id in (%s, %s, %s, %s)",
                (user_id, first_period, service_id_iptv, service_id_internet, service_id_wink, service_id_telephone,))
    main_request_fact = cur.fetchone()[0]

    if main_request_fact == 0:
        result_main_conversion_requests_to_connect_services = 0
    else:
        result_main_conversion_requests_to_connect_services = current_connected_main_services * 100 / main_request_fact

    if result_main_conversion_requests_to_connect_services < 40:
        kpi_conversion_main_services = 0
    elif result_main_conversion_requests_to_connect_services < 50:
        kpi_conversion_main_services = 0.6
    elif result_main_conversion_requests_to_connect_services < 60:
        kpi_conversion_main_services = 0.8
    elif result_main_conversion_requests_to_connect_services < 70:
        kpi_conversion_main_services = 1
    else:
        kpi_conversion_main_services = 1.2

    # Вычисление KPI плана по ДОПОЛНИТЕЛЬНЫМ услугам
    cur.execute("SELECT value FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator_connected_services_slave, login, first_period,))
    result_slave_services_to_plan = cur.fetchone()
    if result_slave_services_to_plan is None:
        result_slave_services_to_plan = 0
    else:
        result_slave_services_to_plan = float(result_slave_services_to_plan[0])

    # Вычисление процента выполнения плана по ДОП услугам
    if result_slave_services_to_plan == 0:
        result_slave_services_percent = 0
    else:
        result_slave_services_percent = current_connected_slave_services * 100 / result_slave_services_to_plan

    if result_slave_services_percent < 50:
        kpi_slave_services = 0
    elif result_slave_services_percent < 80:
        kpi_slave_services = 0.4
    elif result_slave_services_percent < 100:
        kpi_slave_services = 0.7
    else:
        kpi_slave_services = 1

    #  Вычисление KPI конвертации по ДОПОЛНИТЕЛЬНЫМ УСЛУГАМ
    cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE user_id = %s AND date >= %s"
                " and service_id in (%s, %s, %s)",
                (user_id, first_period, service_id_external_camera, service_id_internal_camera, service_id_mobile,))
    slave_request_fact = cur.fetchone()[0]

    if slave_request_fact == 0:
        result_slave_conversion_requests_to_connect_services = 0
    else:
        result_slave_conversion_requests_to_connect_services = current_connected_slave_services * 100 / slave_request_fact

    if result_slave_conversion_requests_to_connect_services < 40:
        kpi_conversion_slave_services = 0
    elif result_slave_conversion_requests_to_connect_services < 50:
        kpi_conversion_slave_services = 0.6
    elif result_slave_conversion_requests_to_connect_services < 60:
        kpi_conversion_slave_services = 0.8
    elif result_slave_conversion_requests_to_connect_services < 70:
        kpi_conversion_slave_services = 1
    else:
        kpi_conversion_slave_services = 1.2

    # ИТОГОВЫЙ КОЭФФИЦИЕНТ МАТЬ ЕГО ВОТ (коэффициент обязательства по приложению № 4):
    kpi_to_application_4 = round(
        (share_of_implementation_kpi_qm * kpi_qm) +
        (share_of_implementation_kpi_plan_main_connect_service * kpi_main_services) +
        (share_of_implementation_kpi_conversion_main * kpi_conversion_main_services) +
        (share_of_implementation_kpi_plan_slave_connect_service * kpi_slave_services) +
        (share_of_implementation_kpi_conversion_slave * kpi_conversion_slave_services), 2)

    # ИТОГОВЫЙ ПОДСЧЕТ:
    #
    income_forecast_sql = payment_to_hours + ((result_payment_main_service + result_payment_slave_service) * kpi_to_application_4)

    # print("\n\033[33mТекущие заявки: ", round(result_all_requests_fact, 0),
    #       "\nПлановое количество заявок: ", round(result_all_requests_to_plan, 0),
    #       "\nПроцент заявок: ", result_all_requests_percent,
    #       "\nСтавка за час по должности: ", rate_the_bablo)
    # print("KPI по норме занятости: ", kpi_employment)
    # print("KPI по заявкам: ", kpi_requests)
    # print("KPI за отработанные часы: ", kpi_online_time)
    # print("KPI по приложению № 4: ", kpi_to_application_4)
    # print("KPI конверсии: ", kpi_conversion)
    # print("оплата за ЧАСЫ: ", payment_to_hours)
    # print("оплата за УСЛУГИ: ", payment_to_connected_services)
    # print("ПРОГНОЗ НА ТЕКУЩИЙ ДЕНЬ: ", income_forecast_sql)
    assert float(income_forecast_method['salary_summary'][8]['value']) == income_forecast_sql
    return payment_to_hours, payment_to_main_connected_services, payment_to_slave_connected_services


# Виджет прогноз дохода с деньгами, ПРОГНОЗ ДОХОДА НА КОНЕЦ МЕСЯЦА
def test_widget_income_forecast_on_end_current_month():
    user_id = id_users()
    indicator = 'rtk_total_payment_current_dynamic'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    id_indicator_connected_services_main = 111
    id_indicator_connected_services_slave = 113

    cur = conn.cursor()
    cur_naumen = connection.cursor()

    id_indicator = 85  # id показателя для получения плана по рабочим часам
    rate_the_bablo = rate_of_the_positions()

    cur_naumen.execute("SELECT SUM(duration) FROM mv_user_online_time_daily WHERE login = %s AND date_work >= %s AND "
                       "status = 'online'",
                       (login, first_day_of_current_period,))
    online_time_current = round(float(cur_naumen.fetchone()[0] / 3600), 0)  # Текущее время в онлайне

    # Получение плана по рабочим часам
    cur.execute("SELECT coalesce(value::float, 0) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator, login, first_period,))
    online_time_plan = cur.fetchone()
    if online_time_plan is None:
        online_time_plan = 0
    else:
        online_time_plan = float(online_time_plan[0]) / 3600

    # ПРОГНОЗ ЧАСОВ!!!!!!!!!
    online_time_current_forecast = online_time_current * kpi_yesterday_and_last_day
    # Вычисление коэффициента по отработанным часам в соответствии с ПРОГНОЗИРУЕМЫМ отработанным временем
    if online_time_plan == 0:
        forecast_online_time_without_kpi_percent_to_plan = 0
    else:
        forecast_online_time_without_kpi_percent_to_plan = online_time_current_forecast * 100 / online_time_plan
    if forecast_online_time_without_kpi_percent_to_plan < 60:
        kpi_online_time = 0
    elif forecast_online_time_without_kpi_percent_to_plan < 80:
        kpi_online_time = 0.5
    elif forecast_online_time_without_kpi_percent_to_plan < 95:
        kpi_online_time = 0.8
    else:
        kpi_online_time = 1

    # Получение нормы занятости
    cur_naumen.execute(
        "select sum(duration) from mv_user_status_full_daily where login = %s and date_work >= %s and "
        "(away_status_reason not in ('CustomAwayReason2', 'CustomAwayReason3', 'PrepareToWork', 'initializing')"
        " or away_status_reason is null) and main_status != 'offline' and (wrapup != 'wrapup' or wrapup is null or away_status_reason is not null) and "
        "(away_status != 'away' or away_status is null or away_status_reason is not null) and (wrapup <> 'wrapup' or "
        "wrapup is null or away_status_reason not in ('CustomAwayReason5', 'CustomAwayReason7', 'CustomAwayReason4'))",
        (login, first_period,))
    result_employment = round(int(cur_naumen.fetchone()[0] / 3600) * 100 / online_time_current, 2)  # норма занятости

    # Вычисление коэффициента по выполнению нормы занятости:
    if result_employment < 50:
        kpi_employment = 0
    elif result_employment < 70:
        kpi_employment = 0.4
    elif result_employment < 85:
        kpi_employment = 0.7
    elif result_employment < 91:
        kpi_employment = 1
    else:
        kpi_employment = 1.2

    # ОПЛАТА ЗА ЧАСЫ ИТОГ: количество отработанных часов * ставка за рабочие часы * кэф за норму занятости * кэф за отработанные часы ПРОГНОЗ
    payment_to_hours = online_time_current * rate_the_bablo * kpi_online_time * kpi_employment

    # тут взяли значения из предыдущего теста
    payment_to_hours_not_interesting, result_payment_main_service, result_payment_slave_service = test_widget_income_forecast_on_current_day()
    payment_to_hours_with_kpi = round(payment_to_hours * kpi_yesterday_and_last_day, 2)

    # Количество услуг СЕЙЧАС ОСНОВНЫЕ
    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE user_id = %s AND date >= %s and "
                "service_id in (%s, %s, %s, %s, %s, %s)",
                (user_id, first_period, service_id_iptv, service_id_internet, service_id_wink, service_id_telephone,
                 service_id_2_in_1, service_id_3_in_1,))
    current_connected_main_services = cur.fetchone()[0]  # Количество основных услуг сейчас

    # Количество услуг СЕЙЧАС ДОПЫ
    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE user_id = %s AND date >= %s and "
                "service_id in (%s, %s, %s)",
                (user_id, first_period, service_id_external_camera, service_id_internal_camera, service_id_mobile,))
    current_connected_slave_services = cur.fetchone()[0]  # Количество дополнительных услуг сейчас
    # Прогноз услуг. И вычисление премиального ПРОНОЗИРУЕМОГО бл коэффициента
    connected_main_services_forecast = current_connected_main_services * kpi_yesterday_and_last_day
    # кэф премиального заработка
    if connected_main_services_forecast == 0:
        kpi_prize_main = 0
    elif connected_main_services_forecast < 31:
        kpi_prize_main = 1
    elif connected_main_services_forecast < 41:
        kpi_prize_main = 1.2
    elif connected_main_services_forecast < 51:
        kpi_prize_main = 1.3
    else:
        kpi_prize_main = 1.4

    # АНалогично по ДОПам
    connected_slave_services_forecast = current_connected_slave_services * kpi_yesterday_and_last_day
    # кэф премиального заработка
    if connected_slave_services_forecast == 0:
        kpi_prize_slave = 0
    elif connected_slave_services_forecast < 5:
        kpi_prize_slave = 1
    elif connected_slave_services_forecast == 5:
        kpi_prize_slave = 1.2
    elif connected_slave_services_forecast == 6:
        kpi_prize_slave = 1.3
    elif connected_slave_services_forecast == 7:
        kpi_prize_slave = 1.4
    else:
        kpi_prize_slave = 1.5

    result_payment_main_service_with_kpi = round(result_payment_main_service * kpi_yesterday_and_last_day * kpi_prize_main, 2)
    result_payment_slave_service_with_kpi = round(result_payment_slave_service * kpi_yesterday_and_last_day * kpi_prize_slave, 2)

    ############### Вычисление основного KPI ########################
    cur.execute("SELECT value FROM operator_rate WHERE day >= %s AND operator_id = %s ORDER BY day DESC",
                (first_period, user_id,))
    qm_value = cur.fetchone()
    if qm_value is not None:
        qm_value = qm_value[0]
    else:
        qm_value = 0

    # Вычисление KPI качества прослушанных диалогов
    if qm_value < 50:
        kpi_qm = 0
    elif qm_value < 60:
        kpi_qm = 0.4
    elif qm_value < 75:
        kpi_qm = 0.7
    else:
        kpi_qm = 1

    # Вычисление KPI плана по ОСНОВНЫМ услугам
    cur.execute("SELECT value FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator_connected_services_main, login, first_period,))
    result_main_services_to_plan = cur.fetchone()
    if result_main_services_to_plan is None:
        result_main_services_to_plan = 0
    else:
        result_main_services_to_plan = float(result_main_services_to_plan[0])

    # количество основных услуг сейчас
    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE user_id = %s AND date >= %s and "
                "service_id in (%s, %s, %s, %s, %s, %s)",
                (user_id, first_period, service_id_iptv, service_id_internet, service_id_wink, service_id_telephone,
                 service_id_2_in_1, service_id_3_in_1,))
    current_connected_main_services = cur.fetchone()[0]  # Количество основных услуг сейчас
    # ПРогноз услуг!!!!!!!!
    current_connected_main_services_forecast = current_connected_main_services * kpi_yesterday_and_last_day
    # Вычисление процента выполнения плана по основным услугам
    if result_main_services_to_plan == 0:
        result_main_services_percent = 0
    else:
        result_main_services_percent = current_connected_main_services_forecast * 100 / result_main_services_to_plan

    if result_main_services_percent < 50:
        kpi_main_services = 0
    elif result_main_services_percent < 80:
        kpi_main_services = 0.4
    elif result_main_services_percent < 100:
        kpi_main_services = 0.7
    else:
        kpi_main_services = 1

    #  Вычисление KPI конвертации по ОСНОВНЫМ УСЛУГАМ
    cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE user_id = %s AND date >= %s"
                " and service_id in (%s, %s, %s, %s, %s, %s)",
                (user_id, first_period, service_id_iptv, service_id_internet, service_id_wink, service_id_telephone,
                 service_id_2_in_1, service_id_3_in_1,))
    main_request_fact = cur.fetchone()[0]

    if main_request_fact == 0:
        result_main_conversion_requests_to_connect_services = 0
    else:
        result_main_conversion_requests_to_connect_services = current_connected_main_services * 100 / main_request_fact

    if result_main_conversion_requests_to_connect_services < 40:
        kpi_conversion_main_services = 0
    elif result_main_conversion_requests_to_connect_services < 50:
        kpi_conversion_main_services = 0.6
    elif result_main_conversion_requests_to_connect_services < 60:
        kpi_conversion_main_services = 0.8
    elif result_main_conversion_requests_to_connect_services < 70:
        kpi_conversion_main_services = 1
    else:
        kpi_conversion_main_services = 1.2

    # Вычисление KPI плана по ДОПОЛНИТЕЛЬНЫМ услугам
    cur.execute("SELECT value FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator_connected_services_slave, login, first_period,))
    result_slave_services_to_plan = cur.fetchone()
    if result_slave_services_to_plan is None:
        result_slave_services_to_plan = 0
    else:
        result_slave_services_to_plan = float(result_slave_services_to_plan[0])

    # Вычисление количества доп услуг сейчас
    cur.execute("SELECT COUNT(id) FROM rtk_connected_services WHERE user_id = %s AND date >= %s and "
                "service_id in (%s, %s, %s)",
                (user_id, first_period, service_id_external_camera, service_id_internal_camera, service_id_mobile,))
    current_connected_slave_services = cur.fetchone()[0]  # Количество дополнительных услуг сейчас

    # ПРОГНОЗ ДОП УСЛУГ
    current_connected_slave_services_forecast = current_connected_slave_services * kpi_yesterday_and_last_day
    # Вычисление процента выполнения плана по ДОП услугам
    if result_slave_services_to_plan == 0:
        result_slave_services_percent = 0
    else:
        result_slave_services_percent = current_connected_slave_services_forecast * 100 / result_slave_services_to_plan

    if result_slave_services_percent < 50:
        kpi_slave_services = 0
    elif result_slave_services_percent < 80:
        kpi_slave_services = 0.4
    elif result_slave_services_percent < 100:
        kpi_slave_services = 0.7
    else:
        kpi_slave_services = 1

    #  Вычисление KPI конвертации по ДОПОЛНИТЕЛЬНЫМ УСЛУГАМ
    cur.execute("SELECT COUNT(id) FROM rtk_requests WHERE user_id = %s AND date >= %s"
                " and service_id in (%s, %s, %s)",
                (user_id, first_period, service_id_external_camera, service_id_internal_camera, service_id_mobile,))
    slave_request_fact = cur.fetchone()[0]

    if slave_request_fact == 0:
        result_slave_conversion_requests_to_connect_services = 0
    else:
        result_slave_conversion_requests_to_connect_services = current_connected_slave_services * 100 / slave_request_fact

    if result_slave_conversion_requests_to_connect_services < 40:
        kpi_conversion_slave_services = 0
    elif result_slave_conversion_requests_to_connect_services < 50:
        kpi_conversion_slave_services = 0.6
    elif result_slave_conversion_requests_to_connect_services < 60:
        kpi_conversion_slave_services = 0.8
    elif result_slave_conversion_requests_to_connect_services < 70:
        kpi_conversion_slave_services = 1
    else:
        kpi_conversion_slave_services = 1.2

    # ИТОГОВЫЙ КОЭФФИЦИЕНТ МАТЬ ЕГО ВОТ (коэффициент обязательства по приложению № 4):
    kpi_to_application_4 = round(
        (share_of_implementation_kpi_qm * kpi_qm) +
        (share_of_implementation_kpi_plan_main_connect_service * kpi_main_services) +
        (share_of_implementation_kpi_conversion_main * kpi_conversion_main_services) +
        (share_of_implementation_kpi_plan_slave_connect_service * kpi_slave_services) +
        (share_of_implementation_kpi_conversion_slave * kpi_conversion_slave_services), 2)

    # ИТОГОВЫЙ ПОДСЧЁТ
    result_test_method = payment_to_hours_with_kpi + ((result_payment_main_service_with_kpi + result_payment_slave_service_with_kpi) * kpi_to_application_4)

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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

    income_forecast_method = requestdict['data'][indicator]['value']  # Результат вычислений с бэкенда


    # print("\n\033[33mПРОГНОЗЫ БАБЛА ЗА УСЛУГИ\nШПД: ", (round(count_service_id_internet / day_of_the_yesterday * last_day_of_the_period, 0) * rate_internet),
    #         "\nIPTV: ", (round(count_service_id_iptv / day_of_the_yesterday * last_day_of_the_period, 0) * rate_iptv),
    #         "\nWINK: ", (round(count_service_id_wink / day_of_the_yesterday * last_day_of_the_period, 0) * rate_wink),
    #         "\nIN камера: ", (round(count_service_id_internal_camera / day_of_the_yesterday * last_day_of_the_period, 0) * rate_internal_camera),
    #         "\nEX камера: ", (round(count_service_id_external_camera / day_of_the_yesterday * last_day_of_the_period, 0) * rate_external_camera),
    #         "\nмобилка: ", (round(count_service_id_mobile / day_of_the_yesterday * last_day_of_the_period, 0) * rate_mobile),
    #         "\nГарантия: ", (round(count_service_id_guarantee / day_of_the_yesterday * last_day_of_the_period, 0) * rate_guarantee),
    #         "\nГарантия премиум: ", (round(count_service_id_guarantee_premium / day_of_the_yesterday * last_day_of_the_period, 0) * rate_guarantee_premium),
    #         "\nУмный дом: ", (round(count_service_id_smart_house / day_of_the_yesterday * last_day_of_the_period, 0) * rate_smart_house),
    #         "\n2 в 1: ", (round(count_service_id_2_in_1 / day_of_the_yesterday * last_day_of_the_period, 0) / 2 * rate_2_in_1),
    #         "\n3 в 1: ", (round(count_service_id_3_in_1 / day_of_the_yesterday * last_day_of_the_period, 0) * rate_3_in_1))
    # print("\n\033[33mПРОГНОЗЫ КОЛИЧЕСТВА УСЛУГ\nШПД: ", (round(count_service_id_internet / day_of_the_yesterday * last_day_of_the_period, 0)),
    #         "\nIPTV: ", (round(count_service_id_iptv / day_of_the_yesterday * last_day_of_the_period, 0)),
    #         "\nWINK: ", (round(count_service_id_wink / day_of_the_yesterday * last_day_of_the_period, 0)),
    #         "\nIN камера: ", (round(count_service_id_internal_camera / day_of_the_yesterday * last_day_of_the_period, 0)),
    #         "\nEX камера: ", (round(count_service_id_external_camera / day_of_the_yesterday * last_day_of_the_period, 0)),
    #         "\nмобилка: ", (round(count_service_id_mobile / day_of_the_yesterday * last_day_of_the_period, 0)),
    #         "\nГарантия: ", (round(count_service_id_guarantee / day_of_the_yesterday * last_day_of_the_period, 0)),
    #         "\nГарантия премиум: ", (round(count_service_id_guarantee_premium / day_of_the_yesterday * last_day_of_the_period, 0)),
    #         "\nУмный дом: ", (round(count_service_id_smart_house / day_of_the_yesterday * last_day_of_the_period, 0)),
    #         "\n2 в 1: ", round((count_service_id_2_in_1 / day_of_the_yesterday * last_day_of_the_period) / 2, 0),
    #         "\n3 в 1: ", (round(count_service_id_3_in_1 / day_of_the_yesterday * last_day_of_the_period, 0)))
    # print("\n\033[33mТекущие заявки: ", round(result_all_requests_fact, 0),
    #       "\nВчерашний день: ", day_of_the_yesterday,
    #       "\nПоследний день месяца: ", day_of_the_end_period,
    #       "\nПрогнозируемое количество заявок: ", round(forecast_requests, 0),
    #       "\nПлановое количество заявок: ", round(result_all_requests_to_plan, 0),
    #       "\nПроцент заявок: ", result_all_requests_percent,
    #       "\nСтавка за час по должности: ", rate_the_bablo,
    #       "\nЛесенка по услугам: ", stairs_rate)
    # print("Спрогшнозированное количество часов", forecast_online_time_without_kpi)
    # print("KPI по норме занятости: ", kpi_employment)
    # print("KPI по заявкам: ", kpi_requests)
    # print("KPI за отработанные часы: ", kpi_online_time)
    # print("KPI по приложению № 4: ", kpi_to_application_4)
    # print("KPI конверсии: ", kpi_conversion)
    # print("оплата за ЧАСЫ: ", payment_to_hours)
    # print("оплата за УСЛУГИ: ", payment_to_connected_services)
    # print("ПРОГНОЗ НА КОНЕЦ МЕСЯЦА: ", income_forecast_sql)
    assert income_forecast_method == round(result_test_method, 2)


def test_widget_income_forecast_all_kpi():
    rate_to_the_hours = rate_of_the_positions()
    cur = conn.cursor()
    id_indicator = 85  # id показателя для получения плана по рабочим часам
    id_indicator_connected_services_main = 111
    id_indicator_connected_services_slave = 113
    indicator = 'rtk_total_payment_end_month_max_kpi'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    user_id = id_users()
    rate_main_service = 450
    rate_slave_service = 650

    body = {
        "indicator_acronim": indicator,
        "object_id": user_id,
        "object_type": object_type,
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

    max_kpi_method = requestdict["data"][indicator]['value']

    # Получение плана по рабочим часам
    cur.execute("SELECT coalesce(value::float, 0) FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator, login, first_period,))
    online_time_plan = cur.fetchone()
    if online_time_plan is None:
        online_time_plan = 0
    else:
        online_time_plan = float(online_time_plan[0]) / 3600

    # Оплата за часы итого
    payment_to_hours = online_time_plan * rate_to_the_hours

    # Вычисление плана по ОСНОВНЫМ услугам
    cur.execute("SELECT value FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator_connected_services_main, login, first_period,))
    result_main_services_to_plan = cur.fetchone()
    if result_main_services_to_plan is None:
        result_main_services_to_plan = 0
    else:
        result_main_services_to_plan = result_main_services_to_plan[0]

    # Вычисление плана по ДОПОЛНИТЕЛЬНЫМ услугам
    cur.execute("SELECT value FROM plan_objects_indicators JOIN objects_indicators ON "
                "plan_objects_indicators.object_indicator_id = objects_indicators.id JOIN users ON "
                "objects_indicators.object_id = users.id WHERE indicator_id = %s AND login = %s AND period_begin = %s",
                (id_indicator_connected_services_slave, login, first_period,))
    result_slave_services_to_plan = cur.fetchone()
    if result_slave_services_to_plan is None:
        result_slave_services_to_plan = 0
    else:
        result_slave_services_to_plan = result_slave_services_to_plan[0]
    payment_to_main_connect_service = result_main_services_to_plan * rate_main_service
    payment_to_slave_connect_service = result_slave_services_to_plan * rate_slave_service
    result_test_method = payment_to_hours + payment_to_slave_connect_service + payment_to_main_connect_service

    # print("\nПлан по рабочим часам: ", work_hours_plan_sql,
    #       "\nСтавка по должности: ", rate_the_bablo,
    #       "\nПодключенные услуги факт: ", connected_services_fact,
    #       "\nПодключенные услуги план: ", connected_services_plan,
    #       "\nKPI за подключенные услуги: ", kpi_connected_services)
    assert result_test_method == max_kpi_method


# закрытие подключения к бд по ssh
def test_stopserver():
    stop(conn, server)
    stop_naumen(connection)
