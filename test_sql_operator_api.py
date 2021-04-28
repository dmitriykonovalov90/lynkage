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


# # старт подключения к бд через ssh
# def test_startserver():
#     global conn, conn_node, server, connection, connection_ora
#     conn, conn_node, server = start()
#     connection = start_naumen()
#     connection_ora = start_oracle()

# def test_start_db(startserver):
#     print('start')
# вычленение id пользователя по имеющемуся логину
# def id_users():
#     cur = conn.cursor()
#     cur.execute("SELECT id FROM users WHERE login = %s", (login,))
#     id_user = cur.fetchone()[0]
#     return id_user


# def fio_users():
#     cur = conn.cursor()
#     cur.execute("SELECT surname, first_name, middle_name FROM users WHERE login = %s", (login,))
#     result = cur.fetchall()[0]
#     fio_user = (' '.join(result))
#     return fio_user


# def uuid_operator():
#     cur = conn.cursor()
#     user_id = id_users()
#     cur.execute('select etl_user_id from users '
#                 'where id = %s',
#                 (user_id,))
#     uuid = cur.fetchone()
#     if uuid is None:
#         uuid = 0
#     else:
#         uuid = uuid[0]
#     return uuid
#
#
# def operator_position_id():
#     cur = conn.cursor()
#     cur.execute("select position_id from users_positions "
#                 "where user_id = %s",
#                 (id_users(),))
#     position_id = cur.fetchone()
#     if position_id is None:
#         position_id = 2
#         return position_id
#     else:
#         return position_id[0]

#
'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО ЗАЯВОК ФАКТ                       '''


def test_operator_requests_created(request_created, id_users):
    indicator = 'rtk_volga_requests_created'

    body = {
            "indicator_acronim": indicator,
            "object_id": id_users,
            "object_type": object_type,
            "parameters":
            {
                "period_begin": first_period,
                "period_end": end_period,
                "organization_id": organization_id
            }
            }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    print('\n\u001B[36mВиджет "Количество заявок", факт')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', request_created)
    assert requestdict['data'][indicator]['value'] == request_created


'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО ЗАЯВОК ПЛАН                      '''


def test_operator_requests_plan(request_plan, id_users):
    indicator = 'rtk_volga_requests_created_plan'

    body = {
            "indicator_acronim": indicator,
            "object_id": id_users,
            "object_type": object_type,
            "parameters":
            {
                "period_begin": first_period,
                "period_end": end_period,
                "organization_id": organization_id
            }
            }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    print('\n\n\u001B[36mВиджет "Количество заявок", план')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', request_plan)
    assert requestdict['data'][indicator]['value'] == request_plan


'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО ЗАЯВОК ПРОЦЕНТЫ                     '''


def test_operator_requests_percent(request_created, request_plan, id_users):
    indicator = 'rtk_volga_requests_created_plan_percent'

    if request_plan is None:
        result_sql = 0
    else:
        result_sql = float(request_created) * 100 / float(request_plan)

    body = {
            "indicator_acronim": indicator,
            "object_id": id_users,
            "object_type": object_type,
            "parameters":
            {
                "period_begin": first_period,
                "period_end": end_period,
                "organization_id": organization_id
            }
            }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    print('\n\n\u001B[36mВиджет "Количество заявок", процент выполнения')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', result_sql)
    assert round(requestdict['data'][indicator]['value'], 5) == round(result_sql, 5)


'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО ПОДКЛЮЧЕННЫХ УСЛУГ ФАКТ                       '''


def test_operator_connected_all_services(connected_all_services, id_users):
    indicator = 'rtk_volga_connected_services_count'

    body = {
            "indicator_acronim": indicator,
            "object_id": id_users,
            "object_type": object_type,
            "parameters":
            {
                "period_begin": first_period,
                "period_end": end_period,
                "organization_id": organization_id
            }
            }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    print('\n\u001B[36mВиджет "Количество подключенных услуг", факт')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', connected_all_services)
    assert requestdict['data'][indicator]['value'] == connected_all_services


'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО ПОДКЛЮЧЕННЫХ УСЛУГ ПЛАН                       '''


def test_operator_connect_services_plan(connect_services_plan, id_users):
    indicator = 'connected_all_services_count_plan'

    body = {
            "indicator_acronim": indicator,
            "object_id": id_users,
            "object_type": object_type,
            "parameters":
            {
                "period_begin": first_period,
                "period_end": end_period,
                "organization_id": organization_id
            }
            }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    print('\n\n\u001B[36mВиджет "Количество подключенных услуг", план')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', connect_services_plan)
    assert requestdict['data'][indicator]['value'] == connect_services_plan


'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО ПОДКЛЮЧЕННЫХ УСЛУГ ПРОЦЕНТ                       '''


def test_operator_all_connect_services_percent(connected_all_services, connect_services_plan, id_users):
    indicator = 'connected_all_services_count_plan_percent'

    if connect_services_plan is None:
        result_sql = 0
    else:
        result_sql = float(connected_all_services) * 100 / float(connect_services_plan)

    body = {
            "indicator_acronim": indicator,
            "object_id": id_users,
            "object_type": object_type,
            "parameters":
            {
                "period_begin": first_period,
                "period_end": end_period,
                "organization_id": organization_id
            }
            }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    print('\n\n\u001B[36mВиджет "Количество подключенных услуг", процент выполнения')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', result_sql)
    assert round(requestdict['data'][indicator]['value'], 5) == round(result_sql, 5)


'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО КОНТАКТОВ ФАКТ                       '''


def test_operator_contacts_count(contacts_count, id_users):
    indicator = 'rtk_volga_contacts_count'

    body = {
            "indicator_acronim": indicator,
            "object_id": id_users,
            "object_type": object_type,
            "parameters":
            {
                "period_begin": first_period,
                "period_end": end_period,
                "organization_id": organization_id
            }
            }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)

    print('\n\u001B[36mВиджет "Количество контактов", факт')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', contacts_count)
    assert requestdict['data'][indicator]['value'] == contacts_count


'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО КОНТАКТОВ ПЛАН                       '''


def test_operator_contacts_count_plan(contacts_count_plan, id_users):
    indicator = 'rtk_volga_contacts_count_plan'

    body = {
            "indicator_acronim": indicator,
            "object_id": id_users,
            "object_type": object_type,
            "parameters":
            {
                "period_begin": first_period,
                "period_end": end_period,
                "organization_id": organization_id
            }
            }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    print('\n\n\u001B[36mВиджет "Количество контактов", план')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', contacts_count_plan)
    assert requestdict['data'][indicator]['value'] == contacts_count_plan


'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО КОНТАКТОВ ПРОЦЕНТ                       '''


def test_operator_contacts_count_percent(contacts_count, contacts_count_plan, id_users):
    indicator = 'rtk_volga_contacts_count_plan_percent'

    if contacts_count_plan is None:
        result_sql = 0
    else:
        result_sql = contacts_count * 100 / contacts_count_plan

    body = {
            "indicator_acronim": indicator,
            "object_id": id_users,
            "object_type": object_type,
            "parameters":
            {
                "period_begin": first_period,
                "period_end": end_period,
                "organization_id": organization_id
            }
            }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    print('\n\n\u001B[36mВиджет "Количество контактов", процент выполнения')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', result_sql)
    assert round(requestdict['data'][indicator]['value'], 5) == round(result_sql, 5)


'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - РАБОЧИЕ ЧАСЫ ФАКТ                       '''


def test_operator_work_hours():
    cur_naumen = connection.cursor()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'rtk_volga_work_hours'

    cur_naumen.execute("""
    select sum(duration) from mv_user_status_full_daily
    where login = %s
    and date_work between %s and %s
    and main_status in ('available', 'notavailable')
     """, (login_non_volga, first_period, end_period,))

    result_sql = cur_naumen.fetchone()[0]

    if result_sql is None:
        result_sql = 0

    body = {
            "indicator_acronim": indicator,
            "object_id": id_users(),
            "object_type": object_type,
            "parameters":
            {
                "period_begin": first_period,
                "period_end": end_period,
                "organization_id": organization_id
            }
            }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)

    print('\n\u001B[36mВиджет "Рабочие часы", факт')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', result_sql, '. В часах - ', result_sql/3600)
    assert requestdict['data'][indicator]['value'] == result_sql
    return result_sql


'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - РАБОЧИЕ ЧАСЫ ПЛАН                       '''


def test_operator_work_hours_plan():
    cur = conn.cursor()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'rtk_volga_work_hours_plan'
    indicator_id = 2941

    cur.execute("""
    select sum(value) from plan_objects_indicators
    join objects_indicators on objects_indicators.id = plan_objects_indicators.object_indicator_id
    join users on users.id = objects_indicators.object_id
    where indicator_id = %s and users.id = %s and period_begin between %s and %s
    """, (indicator_id, id_users(), first_period, end_period,))

    result_sql = cur.fetchone()[0]

    if result_sql is None:
        result_sql = 0

    body = {
            "indicator_acronim": indicator,
            "object_id": id_users(),
            "object_type": object_type,
            "parameters":
            {
                "period_begin": first_period,
                "period_end": end_period,
                "organization_id": organization_id
            }
            }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    print('\n\n\u001B[36mВиджет "Рабочие часы", план')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', result_sql, '. В часах - ', result_sql/3600)
    assert requestdict['data'][indicator]['value'] == result_sql
    return result_sql


'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - РАБОЧИЕ ЧАСЫ ПРОЦЕНТ                       '''


def test_operator_work_hours_percent():
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'rtk_volga_work_hours_plan_percent'
    result_sql_fact = float(test_operator_work_hours())
    result_sql_plan = float(test_operator_work_hours_plan())

    if result_sql_plan is None:
        result_sql = 0
    else:
        result_sql = result_sql_fact * 100 / result_sql_plan

    body = {
            "indicator_acronim": indicator,
            "object_id": id_users(),
            "object_type": object_type,
            "parameters":
            {
                "period_begin": first_period,
                "period_end": end_period,
                "organization_id": organization_id
            }
            }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    print('\n\n\u001B[36mВиджет "Рабочие часы", процент выполнения')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', result_sql)
    assert round(requestdict['data'][indicator]['value'], 5) == round(result_sql, 5)


'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОНВЕРСИЯ ИЗ КОНТАКТОВ В ЗАЯВКИ                       '''


def test_operator_contacts_to_requests_conversion():
    cur_naumen = connection.cursor()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'rtk_volga_contacts_to_requests_conversion'

    cur_naumen.execute("""
    select ((select sum(calls) from mv_user_calls_result_daily
    where login = %s
    and date_work between %s and %s
    and "result" = 'Согласие клиента') * 100)
    /
    (select sum(calls) from mv_user_calls_result_daily
    where login = %s
    and date_work between %s and %s
    and "result" in ('Согласие клиента', 'Отказ клиента'))
     """, (login_non_volga, first_period, end_period, login_non_volga, first_period, end_period,))

    result_sql = cur_naumen.fetchone()[0]

    if result_sql is None:
        result_sql = 0

    body = {
            "indicator_acronim": indicator,
            "object_id": id_users(),
            "object_type": object_type,
            "parameters":
            {
                "period_begin": first_period,
                "period_end": end_period,
                "organization_id": organization_id
            }
            }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    print('\n\n\u001B[36mВиджет "Конверсия из контактов в заявки"')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', result_sql)
    assert round(requestdict['data'][indicator]['value'], 5) == round(float(result_sql), 5)


'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОНВЕРСИЯ ИЗ КОНТАКТОВ В ДОГОВОРЫ                       '''


def test_operator_contacts_to_connected_conversion():
    current_connected_services = test_operator_connected_all_services()
    current_contacts_count = test_operator_contacts_count()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'rtk_volga_contacts_to_connected_conversion'

    if current_contacts_count is None:
        result = 0
    else:
        result = current_connected_services * 100 / current_contacts_count

    body = {
            "indicator_acronim": indicator,
            "object_id": id_users(),
            "object_type": object_type,
            "parameters":
            {
                "period_begin": first_period,
                "period_end": end_period,
                "organization_id": organization_id
            }
            }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    print('\n\n\u001B[36mВиджет "Конверсия из контактов в договоры"')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', result)
    assert round(requestdict['data'][indicator]['value'], 5) == round(float(result), 5)


'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОНВЕРСИЯ ИЗ ЗАЯВОК В ДОГОВОРЫ                       '''


def test_operator_requests_to_connected_conversion():
    current_connected_services = test_operator_connected_all_services()
    current_request_count = test_operator_requests_created()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'rtk_volga_requests_to_connected_conversion'

    if current_connected_services is None:
        result = 0
    else:
        result = current_connected_services * 100 / current_request_count

    body = {
            "indicator_acronim": indicator,
            "object_id": id_users(),
            "object_type": object_type,
            "parameters":
            {
                "period_begin": first_period,
                "period_end": end_period,
                "organization_id": organization_id
            }
            }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    print('\n\n\u001B[36mВиджет "Конверсия из заявок в договоры"')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', result)
    assert round(requestdict['data'][indicator]['value'], 5) == round(float(result), 5)


'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - "ЭФФЕКТИВНОСТЬ"                       '''


def test_operator_efficiency():
    current_work_hours = test_operator_work_hours() / 3600
    current_contacts_count = test_operator_contacts_count()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'rtk_volga_efficiency'

    if current_work_hours is None:
        result = 0
    else:
        result = current_contacts_count / current_work_hours

    body = {
            "indicator_acronim": indicator,
            "object_id": id_users(),
            "object_type": object_type,
            "parameters":
            {
                "period_begin": first_period,
                "period_end": end_period,
                "organization_id": organization_id
            }
            }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    requestdict = json.loads(response.content)
    print('\n\n\u001B[36mВиджет "Эффективность"')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', result)
    assert round(requestdict['data'][indicator]['value'], 5) == round(float(result), 5)


# закрытие подключения к бд по ssh
def test_stopserver():
    stop(conn, conn_node, server)
    stop_naumen(connection)
    stop_oracle(connection_ora)
def test_stop_db(stopserver):
    print('stop')