import btoken
import requests
import json
import db
import datetime
import db_n_rtk
import db_oracle
import math

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


# старт подключения к бд через ssh
def test_startserver():
    global conn, conn_node, server, connection, connection_ora
    conn, conn_node, server = start()
    connection = start_naumen()
    connection_ora = start_oracle()


# вычленение id пользователя по имеющемуся логину
def id_users():
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE login = %s", (login,))
    id_user = cur.fetchone()[0]
    return id_user


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
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО ЗАЯВОК ФАКТ                       '''
#
#
# def test_operator_requests_created():
#     cur_n_rtk = connection.cursor()
#     first_period = str(first_day_of_current_period)
#     end_period = str(last_day_of_current_month)
#     indicator = 'rtk_volga_requests_created'
#     result = 'Согласие клиента'
#
#     cur_n_rtk.execute('select sum(calls) from mv_user_calls_result_daily where '
#                       'login = %s and date_work between %s and %s'
#                       'and result = %s',
#                       (login_non_volga, first_period, end_period, result,))
#
#     result_sql_requests = cur_n_rtk.fetchone()[0]
#
#     if result_sql_requests is None:
#         result_sql_requests = 0
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": object_type,
#             "parameters":
#             {
#                 "period_begin": first_period,
#                 "period_end": end_period,
#                 "organization_id": organization_id
#             }
#             }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     print('\n\u001B[36mВиджет "Количество заявок", факт')
#     print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
#     print('\u001B[33msql:\u001B[0m', result_sql_requests)
#     assert requestdict['data'][indicator]['value'] == result_sql_requests
#     return result_sql_requests
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО ЗАЯВОК ПЛАН                      '''
#
#
# def test_operator_requests_plan():
#     cur = conn.cursor()
#     first_period = str(first_day_of_current_period)
#     end_period = str(last_day_of_current_month)
#     indicator = 'rtk_volga_requests_created_plan'
#     indicator_id = 2876
#
#     cur.execute('select sum(value) from plan_objects_indicators '
#                 'join objects_indicators on objects_indicators.id = plan_objects_indicators.object_indicator_id '
#                 'join users on users.id = objects_indicators.object_id '
#                 'where indicator_id = %s and users.id = %s and period_begin between %s and %s',
#                 (indicator_id, id_users(), first_period, end_period,))
#
#     result_sql_requests = cur.fetchone()[0]
#
#     if result_sql_requests is None:
#         result_sql_requests = 0
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": object_type,
#             "parameters":
#             {
#                 "period_begin": first_period,
#                 "period_end": end_period,
#                 "organization_id": organization_id
#             }
#             }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     print('\n\n\u001B[36mВиджет "Количество заявок", план')
#     print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
#     print('\u001B[33msql:\u001B[0m', result_sql_requests)
#     assert requestdict['data'][indicator]['value'] == result_sql_requests
#     return result_sql_requests
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО ЗАЯВОК ПРОЦЕНТЫ                     '''
#
#
# def test_operator_requests_percent():
#     first_period = str(first_day_of_current_period)
#     end_period = str(last_day_of_current_month)
#     indicator = 'rtk_volga_requests_created_plan_percent'
#     result_request_sql_fact = float(test_operator_requests_created())
#     result_request_sql_plan = float(test_operator_requests_plan())
#
#     if result_request_sql_plan is None:
#         result_sql = 0
#     else:
#         result_sql = result_request_sql_fact * 100 / result_request_sql_plan
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": object_type,
#             "parameters":
#             {
#                 "period_begin": first_period,
#                 "period_end": end_period,
#                 "organization_id": organization_id
#             }
#             }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     print('\n\n\u001B[36mВиджет "Количество заявок", процент выполнения')
#     print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
#     print('\u001B[33msql:\u001B[0m', result_sql)
#     assert round(requestdict['data'][indicator]['value'], 5) == round(result_sql, 5)


'''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО ПОДКЛЮЧЕННЫХ УСЛУГ ФАКТ                       '''


def test_operator_connected_all_services():
    cur_ora_rtk = connection_ora.cursor()
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)
    indicator = 'connected_all_services_count'
    print(id_users())

    cur_ora_rtk.execute("""
    select sum(CNT_ALL) from AGP_V_UNITED_REPORT_RES_LN where AGENT = (:1) and PERIOD in (:1, :1)
     """, ['Cибаев Аскар Артурович', 201912, 201911])

    result_sql_requests = cur_ora_rtk.fetchone()[0]
    print(result_sql_requests)

    if result_sql_requests is None:
        result_sql_requests = 0

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
    print('\n\u001B[36mВиджет "Количество заявок", факт')
    print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
    print('\u001B[33msql:\u001B[0m', result_sql_requests)
    assert requestdict['data'][indicator]['value'] == result_sql_requests
    return result_sql_requests
# # Виджет рабочие часы за месяц факт
# def test_work_hours_of_the_month_fact():
#     cur_naumen = connection.cursor()
#     cur = conn.cursor()
#     first_period = str(first_day_of_current_period)
#     end_period = str(last_day_of_current_month)
#     operator_uuid = uuid_operator()
#     indicator = 'tele2_operator_line_work_hours'
#     cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
#                        "where date_work between %s and %s "
#                        "and online_status = 'online' "
#                        "and partner_uuid = %s "
#                        "and away_status_reason is null "
#                        "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
#                        "and operator_uuid = %s ",
#                         (first_period, end_period, partner_uuid, operator_uuid,))
#
#     result_sql_requests = cur_naumen.fetchone()[0]
#     if result_sql_requests is None:
#         result_sql_requests = 0
#     cur.execute("Select sum(value) from lcentrix_idle_hours where user_id = %s and date between %s and %s",
#                 (id_users(), first_period, end_period,))
#     penalty_hours = cur.fetchone()[0]
#     if penalty_hours is not None:
#         result_sql_requests = result_sql_requests - penalty_hours
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": end_period
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == result_sql_requests, "Результат в виджете НЕ соответствует запросу из БД"
#     return result_sql_requests
#
#
# # Виджет рабочие часы за месяц процент выполнения
# def test_work_hours_of_the_month_percent():
#     work_hours_of_the_month_fact = test_work_hours_of_the_month_fact()
#     work_hours_of_the_month_plan = test_work_hours_of_the_month_plan()
#     first_period = str(first_day_of_current_period)
#     end_period = str(last_day_of_current_month)
#     indicator = 'tele2_operator_line_work_hours_plan_percent'
#
#     if work_hours_of_the_month_plan == 0:
#         result_sql = 0
#     else:
#         result_sql = round(float(work_hours_of_the_month_fact / work_hours_of_the_month_plan * 100), 5)
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": end_period
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     result_method = round(requestdict['data'][indicator]['value'], 5)
#     assert result_method == result_sql, "Результат в виджете НЕ соответствует запросу из БД"
#
#
# # Минимальная выработка за месяц план
# def test_min_production_of_the_month_plan():
#     work_hours_of_the_month_plan = test_work_hours_of_the_month_plan()
#     position_id = operator_position_id()
#     indicator = 'tele2_operator_min_production_plan'
#     first_period = str(first_day_of_current_period)
#     end_period = str(last_day_of_current_month)
#
#     # коэффициент для подсчёта минимальной выработки.
#     if position_id == 2:
#         kpi = 2.5
#     else:
#         kpi = 2
#
#     min_production_plan = work_hours_of_the_month_plan * kpi / 3600
#
#     body = {
#         "indicator_acronim": indicator,
#         "object_id": id_users(),
#         "object_type": 'user',
#         "parameters": {
#             "organization_id": organization_id,
#             "period_begin": first_period,
#             "period_end": end_period
#         }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == min_production_plan
#     return min_production_plan
#
#
# # Минимальная выработка за месяц факт
# def test_min_production_of_the_month_fact():
#     indicator = 'tele2_operator_lcentrix_connected_services'
#     first_period = str(first_day_of_current_period)
#     end_period = str(last_day_of_current_month)
#     cur_naumen = connection.cursor()
#
#     cur_naumen.execute("select coalesce(sum(calls),0) from mv_user_calls_result_daily_n "
#                            "where date_work between %s and %s "
#                            "and partner_uuid = %s "
#                            "and result in ('Услуга подключена', 'Оффер. Услуга подключена') "
#                            "and operator_uuid = %s",
#                             (first_period, end_period, partner_uuid, uuid_operator(),))
#
#     min_production_fact = cur_naumen.fetchone()
#     if min_production_fact is None:
#         min_production_fact = 0
#     else:
#         min_production_fact = min_production_fact[0]
#
#     body = {
#         "indicator_acronim": indicator,
#         "object_id": id_users(),
#         "object_type": 'user',
#         "parameters": {
#             "organization_id": organization_id,
#             "period_begin": first_period,
#             "period_end": end_period
#         }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == min_production_fact
#     return min_production_fact
#
#
# # Минимальная выработка за месяц процент
# def test_min_production_of_the_month_percent():
#     min_production_plan = test_min_production_of_the_month_plan()
#     min_production_fact = test_min_production_of_the_month_fact()
#     indicator = 'tele2_operator_min_production_plan_percent'
#     first_period = str(first_day_of_current_period)
#     end_period = str(last_day_of_current_month)
#
#     min_production_percent = float(min_production_fact) / min_production_plan * 100
#
#     body = {
#         "indicator_acronim": indicator,
#         "object_id": id_users(),
#         "object_type": 'user',
#         "parameters": {
#             "organization_id": organization_id,
#             "period_begin": first_period,
#             "period_end": end_period
#         }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == min_production_percent
#
#
# # Виджет рабочие часы на текущий день накопленным итогом план
# def test_work_hours_on_the_current_day_plan():
#     cur = conn.cursor()
#     first_period = str(first_day_of_current_period)
#     current_day = str(date_current)
#     indicator = 'tele2_operator_line_work_hours_current_day_plan'
#     indicator_id = 1990
#     cur.execute('select sum(value::int)  from plan_objects_indicators join '
#                 'objects_indicators on plan_objects_indicators.object_indicator_id = objects_indicators.id '
#                 'join users on objects_indicators.object_id = users.id '
#                 'where period_begin between %s and %s '
#                 'and object_id = %s'
#                 'and objects_indicators.indicator_id = %s',
#                 (first_day_of_current_period, current_day, id_users(), indicator_id,))
#
#     result_sql_requests = cur.fetchone()[0]
#     if result_sql_requests is None:
#         result_sql_requests = 0
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": current_day
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == result_sql_requests, "Результат в виджете НЕ соответствует запросу из БД"
#     return result_sql_requests
#
#
# # Виджет рабочие часы на текущий день накопленным итогом факт
# def test_work_hours_on_the_current_day_fact():
#     cur_naumen = connection.cursor()
#     cur = conn.cursor()
#     first_period = str(first_day_of_current_period)
#     current_day = str(date_current)
#     operator_uuid = uuid_operator()
#     indicator = 'tele2_operator_line_work_hours_current_day'
#     cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
#                        "where date_work between %s and %s "
#                        "and online_status = 'online' "
#                        "and partner_uuid = %s "
#                        "and away_status_reason is null "
#                        "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
#                        "and operator_uuid = %s ",
#                         (first_period, current_day, partner_uuid, operator_uuid,))
#
#     result_sql_requests = cur_naumen.fetchone()[0]
#     if result_sql_requests is None:
#         result_sql_requests = 0
#     cur.execute("Select sum(value) from lcentrix_idle_hours where user_id = %s and date between %s and %s",
#                 (id_users(), first_period, current_day,))
#     penalty_hours = cur.fetchone()[0]
#     if penalty_hours is not None:
#         result_sql_requests = result_sql_requests - penalty_hours
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": current_day
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == result_sql_requests, "Результат в виджете НЕ соответствует запросу из БД"
#     return result_sql_requests
#
#
# # Виджет рабочие часы на текущий день накопленным итогом процент выполнения
# def test_work_hours_on_the_current_day_percent():
#     work_hours_on_the_current_day_fact = test_work_hours_on_the_current_day_fact()
#     work_hours_on_the_current_day_plan = test_work_hours_on_the_current_day_plan()
#     first_period = str(first_day_of_current_period)
#     current_day = str(date_current)
#     indicator = 'tele2_operator_line_work_hours_current_day_plan_percent'
#
#     if work_hours_on_the_current_day_plan == 0:
#         result_sql = 0
#     else:
#         result_sql = round(float(work_hours_on_the_current_day_fact / work_hours_on_the_current_day_plan * 100), 5)
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": current_day
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     result_method = round(requestdict['data'][indicator]['value'], 5)
#     assert result_method == result_sql, "Результат в виджете НЕ соответствует запросу из БД"
#
#
# # Минимальная выработка на текущий день накопленным итогом план
# def test_min_production_on_the_period_to_current_day_plan():
#     work_hours_on_the_current_day_plan = test_work_hours_on_the_current_day_plan()
#     position_id = operator_position_id()
#     indicator = 'tele2_operator_min_production_current_day_plan'
#     first_period = str(first_day_of_current_period)
#     current_day = str(date_current)
#
#     # коэффициент для подсчёта минимальной выработки.
#     if position_id == 2:
#         kpi = 2.5
#     else:
#         kpi = 2
#
#     min_production_plan = work_hours_on_the_current_day_plan * kpi / 3600
#
#     body = {
#         "indicator_acronim": indicator,
#         "object_id": id_users(),
#         "object_type": 'user',
#         "parameters": {
#             "organization_id": organization_id,
#             "period_begin": first_period,
#             "period_end": current_day
#         }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == min_production_plan
#     return min_production_plan
#
#
# # Минимальная выработка на текущий день накопленным итогом факт
# def test_min_production_on_the_period_to_current_day_fact():
#     indicator = 'tele2_operator_min_production_current_day'
#     first_period = str(first_day_of_current_period)
#     current_day = str(date_current)
#     cur_naumen = connection.cursor()
#
#     cur_naumen.execute("select coalesce(sum(calls),0) from mv_user_calls_result_daily_n "
#                            "where date_work between %s and %s "
#                            "and partner_uuid = %s "
#                            "and result in ('Услуга подключена', 'Оффер. Услуга подключена') "
#                            "and operator_uuid = %s",
#                             (first_period, current_day, partner_uuid, uuid_operator(),))
#
#     min_production_fact = cur_naumen.fetchone()
#     if min_production_fact is None:
#         min_production_fact = 0
#     else:
#         min_production_fact = min_production_fact[0]
#
#     body = {
#         "indicator_acronim": indicator,
#         "object_id": id_users(),
#         "object_type": 'user',
#         "parameters": {
#             "organization_id": organization_id,
#             "period_begin": first_period,
#             "period_end": current_day
#         }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == min_production_fact
#     return min_production_fact
#
#
# # Минимальная выработка на текущий день накопленным итогом процент
# def test_min_production_on_the_period_to_current_day_percent():
#     min_production_plan = int(test_min_production_on_the_period_to_current_day_plan())
#     min_production_fact = test_min_production_of_the_month_fact()
#     indicator = 'tele2_operator_min_production_current_day_plan_percent'
#     first_period = str(first_day_of_current_period)
#     current_day = str(date_current)
#     if min_production_plan != 0:
#         min_production_percent = float(min_production_fact) / min_production_plan * 100
#     else:
#         min_production_percent = 0
#
#     body = {
#         "indicator_acronim": indicator,
#         "object_id": id_users(),
#         "object_type": 'user',
#         "parameters": {
#             "organization_id": organization_id,
#             "period_begin": first_period,
#             "period_end": current_day
#         }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == min_production_percent
#
#
# # Виджет рабочие часы ЗА текущий день план
# def test_work_hours_for_the_current_day_plan():
#     cur = conn.cursor()
#     current_day = str(date_current)
#     indicator = 'tele2_operator_line_work_hours_this_day_plan'
#     indicator_id = 1990
#     cur.execute('select sum(value::int)  from plan_objects_indicators join '
#                 'objects_indicators on plan_objects_indicators.object_indicator_id = objects_indicators.id '
#                 'join users on objects_indicators.object_id = users.id '
#                 'where period_begin = %s '
#                 'and object_id = %s'
#                 'and objects_indicators.indicator_id = %s',
#                 (current_day, id_users(), indicator_id,))
#
#     result_sql_requests = cur.fetchone()[0]
#     if result_sql_requests is None:
#         result_sql_requests = 0
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": current_day,
#                 "period_end": current_day
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == result_sql_requests, "Результат в виджете НЕ соответствует запросу из БД"
#     return result_sql_requests
#
#
# # Виджет рабочие часы ЗА текущий день факт
# def test_work_hours_for_the_current_day_fact():
#     cur_naumen = connection.cursor()
#     cur = conn.cursor()
#     current_day = str(date_current)
#     operator_uuid = uuid_operator()
#     indicator = 'tele2_operator_line_work_hours_this_day'
#     cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
#                        "where date_work = %s "
#                        "and online_status = 'online' "
#                        "and partner_uuid = %s "
#                        "and away_status_reason is null "
#                        "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
#                        "and operator_uuid = %s ",
#                         (current_day, partner_uuid, operator_uuid,))
#
#     result_sql_requests = cur_naumen.fetchone()[0]
#     if result_sql_requests is None:
#         result_sql_requests = 0
#     cur.execute("Select sum(value) from lcentrix_idle_hours where user_id = %s and date = %s",
#                 (id_users(), current_day,))
#     penalty_hours = cur.fetchone()[0]
#     if penalty_hours is not None:
#         result_sql_requests = result_sql_requests - penalty_hours
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": current_day,
#                 "period_end": current_day
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == result_sql_requests, "Результат в виджете НЕ соответствует запросу из БД"
#     return result_sql_requests
#
#
# # Виджет рабочие часы ЗА текущий день процент выполнения
# def test_work_hours_for_the_current_day_percent():
#     work_hours_for_the_current_day_fact = test_work_hours_for_the_current_day_fact()
#     work_hours_for_the_current_day_plan = test_work_hours_for_the_current_day_plan()
#     current_day = str(date_current)
#     indicator = 'tele2_operator_line_work_hours_this_day_plan_percent'
#
#     if work_hours_for_the_current_day_plan == 0:
#         result_sql = 0
#     else:
#         result_sql = round(float(work_hours_for_the_current_day_fact / work_hours_for_the_current_day_plan * 100), 5)
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": current_day,
#                 "period_end": current_day
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     result_method = round(requestdict['data'][indicator]['value'], 5)
#     assert result_method == result_sql, "Результат в виджете НЕ соответствует запросу из БД"
#
#
# # Минимальная выработка ЗА текущий день план
# def test_min_production_for_the_current_day_plan():
#     work_hours_for_the_current_day_plan = test_work_hours_for_the_current_day_plan()
#     position_id = operator_position_id()
#     indicator = 'tele2_min_production_this_day_plan'
#     current_day = str(date_current)
#
#     # коэффициент для подсчёта минимальной выработки.
#     if position_id == 2:
#         kpi = 2.5
#     else:
#         kpi = 2
#
#     min_production_plan = work_hours_for_the_current_day_plan * kpi / 3600
#
#     body = {
#         "indicator_acronim": indicator,
#         "object_id": id_users(),
#         "object_type": 'user',
#         "parameters": {
#             "organization_id": organization_id,
#             "period_begin": current_day,
#             "period_end": current_day
#         }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == min_production_plan
#     return min_production_plan
#
#
# # Минимальная выработка за текущий день факт
# def test_min_production_for_the_current_day_fact():
#     indicator = 'tele2_min_production_this_day'
#     current_day = str(date_current)
#     cur_naumen = connection.cursor()
#
#     cur_naumen.execute("select coalesce(sum(calls),0) from mv_phone_call_today "
#                            "where date_work = %s "
#                            "and partner_uuid = %s "
#                            "and result in ('Услуга подключена', 'Оффер. Услуга подключена') "
#                            "and operator_uuid = %s",
#                             (current_day, partner_uuid, uuid_operator(),))
#
#     min_production_fact = cur_naumen.fetchone()
#     if min_production_fact is None:
#         min_production_fact = 0
#     else:
#         min_production_fact = min_production_fact[0]
#
#     body = {
#         "indicator_acronim": indicator,
#         "object_id": id_users(),
#         "object_type": 'user',
#         "parameters": {
#             "organization_id": organization_id,
#             "period_begin": current_day,
#             "period_end": current_day
#         }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == min_production_fact
#     return min_production_fact
#
#
# # Минимальная выработка ЗА текущий день процент
# def test_min_production_for_the_current_day_percent():
#     min_production_plan = test_min_production_for_the_current_day_plan()
#     min_production_fact = test_min_production_for_the_current_day_fact()
#     indicator = 'tele2_min_production_this_day_plan_percent'
#     current_day = str(date_current)
#
#     if min_production_plan == 0:
#         min_production_percent = 0
#     else:
#         min_production_percent = float(min_production_fact) / min_production_plan * 100
#
#     body = {
#         "indicator_acronim": indicator,
#         "object_id": id_users(),
#         "object_type": 'user',
#         "parameters": {
#             "organization_id": organization_id,
#             "period_begin": current_day,
#             "period_end": current_day
#         }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == min_production_percent
#
#
# # прогноз дохода на текущий день
# def test_operator_salary():
#     first_period = str(first_day_of_current_period)
#     indicator1 = 'tele2_service_payment_operator'
#     indicator2 = 'tele2_work_hours_payment_operator'
#     current_day = str(date_current)
#     rate_to_the_hours = 110
#     work_hours_current = test_work_hours_of_the_month_fact() / 3600
#     sales_to_the_month_fact = float(test_min_production_of_the_month_fact())
#
#     # Оплата за часы
#     payment_work_hours_sql = float(rate_to_the_hours * work_hours_current)
#
#     kpi = sales_to_the_month_fact / work_hours_current
#     # вычисление ставки за услуги
#     if kpi < 2.5:
#         rate_service = 0
#     elif kpi < 2.8:
#         rate_service = 25
#     elif kpi < 3.1:
#         rate_service = 30
#     elif kpi < 3.4:
#         rate_service = 34
#     elif kpi < 4:
#         rate_service = 40
#     elif kpi < 4.3:
#         rate_service = 43
#     else:
#         rate_service = 50
#
#     # вычисление выполнения плана Если выполнен, то выплатят зп за услуги, если нет, то херушки
#     kpi_sales = float(sales_to_the_month_fact) / float(test_min_production_of_the_month_plan())
#     if kpi_sales >= 1:
#         kpi_plan = 1
#     else:
#         kpi_plan = 0
#
#     # ЗП за услуги = продажи за месяц факт * ставку за услуги * Выполнили_план_или_нет * конверсия (пока считаем её 1)
#     payment_services = (float(sales_to_the_month_fact) * float(rate_service) * float(kpi_plan) * 1)
#
#     # ВЫЧИСЛЕНИЕ ПОЛНОЙ ЗП
#     # operator_salary = payment_services + payment_work_hours_sql
#
#     # запрос на первую часть зп за услуги
#     body = {
#             "indicator_acronim": indicator1,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": current_day
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict1 = json.loads(response.content)
#     assert requestdict1['data'][indicator1]['value'] == payment_services, "Результат в виджете НЕ соответствует запросу из БД"
#
#     # запрос на вторую часть зп за часы
#     body = {
#         "indicator_acronim": indicator2,
#         "object_id": id_users(),
#         "object_type": 'user',
#         "parameters": {
#             "organization_id": organization_id,
#             "period_begin": first_period,
#             "period_end": current_day
#         }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict2 = json.loads(response.content)
#     assert round(requestdict2['data'][indicator2]['value'], 5) == (round(payment_work_hours_sql, 5)), "Результат в виджете НЕ соответствует запросу из БД"
#
#
# # прогноз дохода при выполнении плана по часам
# def test_operator_forecast_salary():
#     first_period = str(first_day_of_current_period)
#     indicator = 'tele2_total_payment_current_dynamic'
#     last_day = str(last_day_of_current_month)
#     rate_to_the_hours = 110
#     work_hours_plan = float(test_work_hours_of_the_month_plan() / 3600)
#     sales_to_the_month_fact = float(test_min_production_of_the_month_fact())
#     work_hours_current = float(test_work_hours_of_the_month_fact() / 3600)
#     min_production_of_the_month = float(test_min_production_of_the_month_plan())
#
#     # Оплата за часы
#     payment_work_hours_sql = rate_to_the_hours * work_hours_plan
#
#     kpi = sales_to_the_month_fact / work_hours_current
#     # вычисление ставки за услуги
#     if kpi < 2.5:
#         rate_service = 0
#     elif kpi < 2.8:
#         rate_service = 25
#     elif kpi < 3.1:
#         rate_service = 30
#     elif kpi < 3.4:
#         rate_service = 34
#     elif kpi < 4:
#         rate_service = 40
#     elif kpi < 4.3:
#         rate_service = 43
#     else:
#         rate_service = 50
#
#     # вычисление выполнения плана Если выполнен, то выплатят зп за услуги, если нет, то херушки
#     kpi_sales = float(sales_to_the_month_fact) / float(min_production_of_the_month)
#     if kpi_sales >= 1:
#         kpi_plan = 1
#     else:
#         kpi_plan = 0
#
#     # ЗП за услуги = (продажи за месяц факт / рабочие часы факт * рабочие часы план месяц) * ставку за услуги * Выполнили_план_или_нет * конверсия (пока считаем её 1)
#     payment_services_sql = min_production_of_the_month * rate_service * kpi_plan * 1
#
#     # ВЫЧИСЛЕНИЕ ПОЛНОЙ ЗП
#     operator_salary = payment_services_sql + payment_work_hours_sql
#
#     # запрос на первую часть зп за услуги
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": last_day
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == operator_salary, "Результат в виджете НЕ соответствует запросу из БД"
#
#
# # прогноз дохода на текущий день попап детально
# def test_operator_salary_detailed():
#     first_period = str(first_day_of_current_period)
#     indicator = 'tele2_payment_detailed'
#     last_day = str(last_day_of_current_month)
#     rate_to_the_hours = 110
#     work_hours_current = test_work_hours_of_the_month_fact() / 3600
#     sales_to_the_month_fact = float(test_min_production_of_the_month_fact())
#
#     # Оплата за часы
#     payment_work_hours_sql = float(rate_to_the_hours * work_hours_current)
#     kpi = sales_to_the_month_fact / work_hours_current
#     # вычисление ставки за услуги
#     if kpi < 2:
#         rate_service = 0
#     elif kpi < 2.5 and operator_position_id() != 2:
#         rate_service = 15
#     elif kpi < 2.8:
#         rate_service = 25
#     elif kpi < 3.1:
#         rate_service = 30
#     elif kpi < 3.4:
#         rate_service = 34
#     elif kpi < 4:
#         rate_service = 40
#     elif kpi < 4.3:
#         rate_service = 43
#     else:
#         rate_service = 50
#
#     # вычисление выполнения плана Если выполнен, то выплатят зп за услуги, если нет, то херушки
#     kpi_sales = float(sales_to_the_month_fact) / float(test_min_production_of_the_month_plan())
#     if kpi_sales >= 1:
#         kpi_plan = 1
#     else:
#         kpi_plan = 0
#
#     # ЗП за услуги = продажи за месяц факт * ставку за услуги * Выполнили_план_или_нет * конверсия (пока считаем её 1)
#     payment_services = sales_to_the_month_fact * rate_service * kpi_plan * 1
#     payment_services = float(payment_services)
#     # ВЫЧИСЛЕНИЕ ПОЛНОЙ ЗП
#     operator_salary = round(payment_services + float(payment_work_hours_sql), 2)
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": last_day
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert float(requestdict['data'][indicator]['value']['hours_payment'][-1]['value'][0:-5]) == round(payment_work_hours_sql, 2)
#     assert float(requestdict['data'][indicator]['value']['services_payment'][-1]['value'][0:-5]) == round(payment_services, 2)
#     assert requestdict['data'][indicator]['value']['salary_summary_rub'] == operator_salary
#     assert float(requestdict['data'][indicator]['value']['salary_summary'][-1]['value'][0:-5]) == round(operator_salary, 2)
#
#
# # Виджет занятость - занятость
# def test_employment_work():
#     cur_naumen = connection.cursor()
#     cur = conn.cursor()
#     first_period = str(first_day_of_current_period)
#     end_period = str(last_day_of_current_month)
#     operator_uuid = uuid_operator()
#     indicator = 'lcentrix_employment_detail_work'
#     cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
#                        "where date_work between %s and %s "
#                        "and partner_uuid = %s "
#                        "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
#                        "and away_status_reason is null "
#                        "and operator_uuid = %s",
#                         (first_period, end_period, partner_uuid, operator_uuid,))
#
#     result_sql_work = cur_naumen.fetchone()[0]
#     cur.execute("Select sum(value) from lcentrix_idle_hours where user_id = %s and date between %s and %s",
#                 (id_users(), first_period, end_period,))
#     penalty_hours = cur.fetchone()[0]
#     if penalty_hours is not None:
#         result_sql_work = result_sql_work - penalty_hours
#     cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
#                        "where date_work between %s and %s "
#                        "and partner_uuid = %s "
#                        "and away_status_reason = 'CustomAwayReason2' "
#                        "and operator_uuid = %s",
#                         (first_period, end_period, partner_uuid, operator_uuid,))
#
#     result_sql_away = cur_naumen.fetchone()[0]
#
#     result_100_employment = result_sql_work + result_sql_away
#
#     result_sql = round(float(result_sql_work * 100 / result_100_employment), 2)
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": end_period
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value']['value'] == result_sql, "Результат в виджете НЕ соответствует запросу из БД"
#
#
# # Виджет занятость - ОТДЫХ
# def test_employment_away():
#     first_period = str(first_day_of_current_period)
#     end_period = str(last_day_of_current_month)
#     indicator = 'lcentrix_employment_detail_away'
#     cur_naumen = connection.cursor()
#     cur = conn.cursor()
#     operator_uuid = uuid_operator()
#     cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
#                        "where date_work between %s and %s "
#                        "and partner_uuid = %s "
#                        "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
#                        "and away_status_reason is null "
#                        "and operator_uuid = %s",
#                        (first_period, end_period, partner_uuid, operator_uuid,))
#
#     result_sql_work = cur_naumen.fetchone()[0]
#     cur.execute("Select sum(value) from lcentrix_idle_hours where user_id = %s and date between %s and %s",
#                 (id_users(), first_period, end_period,))
#     penalty_hours = cur.fetchone()[0]
#     if penalty_hours is not None:
#         result_sql_work = result_sql_work - penalty_hours
#     cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
#                        "where date_work between %s and %s "
#                        "and partner_uuid = %s "
#                        "and away_status_reason = 'CustomAwayReason2' "
#                        "and operator_uuid = %s",
#                        (first_period, end_period, partner_uuid, operator_uuid,))
#
#     result_sql_away = cur_naumen.fetchone()[0]
#
#     result_100_employment = result_sql_work + result_sql_away
#
#     result_sql = round(float(result_sql_away * 100 / result_100_employment), 2)
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": end_period
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value']['value'] == result_sql, "Результат в виджете НЕ соответствует запросу из БД"
#
#
# # Виджет занятость - ОТДЫХ
# def test_employment_training():
#     first_period = str(first_day_of_current_period)
#     end_period = str(last_day_of_current_month)
#     indicator = 'lcentrix_employment_detail_training'
#     hardcode_value = 0
#     # cur_naumen = connection.cursor()
#     # cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n c "
#     #                    "where c.date_work between %s and %s "
#     #                    "and partner_uuid = %s "
#     #                    "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
#     #                    "and away_status_reason is null "
#     #                    "and c.login in "
#     #                    "(select p.login from dblink"
#     #                    "('host=192.168.77.123 dbname=api user=dashboard_user password=password',"
#     #                    " 'select login from users where salegroup_id = %s') "
#     #                    "as p (login varchar(256)))",
#     #                    (first_period, end_period, partner_uuid, salegroup_id,))
#     #
#     # result_sql_work = cur_naumen.fetchone()[0]
#     #
#     # cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n c "
#     #                    "where c.date_work between %s and %s "
#     #                    "and partner_uuid = %s "
#     #                    "and away_status_reason = 'CustomAwayReason2' "
#     #                    "and c.login in "
#     #                    "(select p.login from dblink"
#     #                    "('host=192.168.77.123 dbname=api user=dashboard_user password=password',"
#     #                    " 'select login from users where salegroup_id = %s') "
#     #                    "as p (login varchar(256)))",
#     #                    (first_period, end_period, partner_uuid, salegroup_id,))
#     #
#     # result_sql_away = cur_naumen.fetchone()[0]
#     #
#     # cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n c "
#     #                    "where c.date_work between %s and %s "
#     #                    "and partner_uuid = %s "
#     #                    "and away_status_reason = 'CustomAwayReason1' "
#     #                    "and c.login in "
#     #                    "(select p.login from dblink"
#     #                    "('host=192.168.77.123 dbname=api user=dashboard_user password=password',"
#     #                    " 'select login from users where salegroup_id = %s') "
#     #                    "as p (login varchar(256)))",
#     #                    (first_period, end_period, partner_uuid, salegroup_id,))
#
#     # result_sql_training = cur_naumen.fetchone()[0]
#     #
#     # result_100_employment = result_sql_work + result_sql_training + result_sql_away
#     #
#     # result_sql = round(float(result_sql_training * 100 / result_100_employment), 2)
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": end_period
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value']['value'] == hardcode_value, "Результат в виджете НЕ соответствует запросу из БД"
#
#
# def get_array_sales_everyday_fact(day):
#     cur_naumen = connection.cursor()
#     array_fact = []
#     date_current_string = str(date_current)
#
#     if day == date_current_string:
#         cur_naumen.execute("select sum(calls) from mv_phone_call_today "
#                                "where date_work = %s "
#                                "and partner_uuid = %s "
#                                "and result in ('Услуга подключена', 'Оффер. Услуга подключена') "
#                                "and operator_uuid = %s",
#                                 (day, partner_uuid, uuid_operator(),))
#         array_fact.append(cur_naumen.fetchone()[0])
#     else:
#         cur_naumen.execute("select sum(calls) from mv_user_calls_result_daily_n "
#                            "where date_work = %s "
#                            "and partner_uuid = %s "
#                            "and result in ('Услуга подключена', 'Оффер. Услуга подключена') "
#                            "and operator_uuid = %s",
#                            (day, partner_uuid, uuid_operator(),))
#         array_fact.append(cur_naumen.fetchone()[0])
#     return array_fact
#
#
# def get_array_work_hours_everyday_fact(day):
#     cur_naumen = connection.cursor()
#     cur = conn.cursor()
#     array_fact = []
#     cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
#                                "where date_work = %s "
#                                "and partner_uuid = %s "
#                                "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
#                                 "and away_status_reason is null "
#                                "and operator_uuid = %s",
#                                 (day, partner_uuid, uuid_operator(),))
#     result = cur_naumen.fetchone()[0]
#     if result is None:
#         result = 0
#     cur.execute("Select sum(value) from lcentrix_idle_hours where user_id = %s and date = %s",
#                 (id_users(), day))
#     penalty_hours = cur.fetchone()[0]
#     if penalty_hours is not None:
#         result = result - penalty_hours
#     array_fact.append(int(result))
#     return array_fact
#
#
# def get_array_sales_everyday_plan(day):
#     day_for_work_hours = day
#     work_hours_plan_everyday = get_array_working_hours_everyday_plan(day_for_work_hours)
#     position = operator_position_id()
#
#     if position == 2:
#         kpi_positions = 2.5
#     else:
#         kpi_positions = 2
#
#     sales_everyday_plan = kpi_positions * (work_hours_plan_everyday[0] / 3600)
#     return sales_everyday_plan
#
#
# def get_array_working_hours_everyday_plan(day):
#     cur = conn.cursor()
#     array_fact = []
#     indicator_id = 1990
#     cur.execute('select sum(value::int)  from plan_objects_indicators join '
#                 'objects_indicators on plan_objects_indicators.object_indicator_id = objects_indicators.id '
#                 'join users on objects_indicators.object_id = users.id '
#                 'where period_begin = %s '
#                 'and object_id = %s'
#                 'and objects_indicators.indicator_id = %s',
#                 (day, id_users(), indicator_id,))
#     result = cur.fetchone()[0]
#     if result is None:
#         result = 0
#     array_fact.append(int(result))
#     return array_fact
#
#
# # Виджет Рабочее время
# def test_working_hours_graph():
#     global first_day_of_current_period
#     first_period = str(first_day_of_current_period)
#     end_period = str(last_day_of_current_month)
#     indicator = 'tele2_operator_line_work_hours_daily_graph'
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": end_period
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#
#     # Сравнение РАБОЧИХ ЧАСОВ ФАКТОВ ЗА КАЖДЫЙ ДЕНЬ
#     iteration = 1
#     first_day_of_current_period = date_current.replace(day=1)
#     while iteration <= last_day_of_current_month.day:
#         first_period = str(first_day_of_current_period)
#         work_hours_fact_sql = get_array_work_hours_everyday_fact(first_period)
#         if work_hours_fact_sql[0] != 0:
#             assert work_hours_fact_sql[0] == requestdict['data'][indicator]['value'][first_period]['fact']
#         first_day_of_current_period = first_day_of_current_period + datetime.timedelta(days=+1)
#         iteration = iteration + 1
#
#     # Сравнение ПРОДАЖ ФАКТОВ ЗА КАЖДЫЙ ДЕНЬ
#     iteration = 1
#     first_day_of_current_period = date_current.replace(day=1)
#     while iteration <= date_current.day:
#         first_period = str(first_day_of_current_period)
#         sales_fact_sql = get_array_sales_everyday_fact(first_period)
#         if sales_fact_sql[0] is not None:
#             assert sales_fact_sql[0] == requestdict['data'][indicator]['value'][first_period]['fact_min_production']
#         first_day_of_current_period = first_day_of_current_period + datetime.timedelta(days=+1)
#         iteration = iteration + 1
#
#     # Сравнение РАБОЧИХ ЧАСОВ ЗА КАЖДЫЙ ДЕНЬ ПЛАН
#     iteration = 1
#     first_day_of_current_period = date_current.replace(day=1)
#     while iteration <= last_day_of_current_month.day:
#         first_period = str(first_day_of_current_period)
#         work_hours_plan_sql = get_array_working_hours_everyday_plan(first_period)
#         if work_hours_plan_sql[0] != 0:
#             assert work_hours_plan_sql[0] == requestdict['data'][indicator]['value'][first_period]['plan']
#         first_day_of_current_period = first_day_of_current_period + datetime.timedelta(days=+1)
#         iteration = iteration + 1
#
#     # Сравнение ПРОДАЖ ЗА КАЖДЫЙ ДЕНЬ ПЛАН
#     iteration = 1
#     first_day_of_current_period = date_current.replace(day=1)
#     while iteration <= last_day_of_current_month.day:
#         first_period = str(first_day_of_current_period)
#         sales_plan_sql = get_array_sales_everyday_plan(first_period)
#         if sales_plan_sql != 0:
#             assert sales_plan_sql == requestdict['data'][indicator]['value'][first_period]['plan_min_production']
#         first_day_of_current_period = first_day_of_current_period + datetime.timedelta(days=+1)
#         iteration = iteration + 1
#
#
# # Виджет эффективность графический
# def test_efficiency_daily_graph():
#     global first_day_of_current_period
#     first_period = str(date_current.replace(day=1))
#     end_period = str(last_day_of_current_month)
#     indicator = 'tele2_operator_efficiency_daily_graph'
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": end_period
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#
#     iteration = 1
#     first_day_of_current_period = date_current.replace(day=1)
#     while iteration <= date_current.day:
#         first_period = str(first_day_of_current_period)
#         sales_fact_sql = get_array_sales_everyday_fact(first_period)
#         work_hours_fact_sql = get_array_work_hours_everyday_fact(first_period)
#         if work_hours_fact_sql[0] == 0:
#             result_efficiency_sql = 0
#         else:
#             result_efficiency_sql = float(sales_fact_sql[0]) / (work_hours_fact_sql[0] / 3600)
#         if result_efficiency_sql != 0:
#             assert round(result_efficiency_sql, 5) == round(requestdict['data'][indicator]['value'][first_period]['fact'], 5)
#         first_day_of_current_period = first_day_of_current_period + datetime.timedelta(days=+1)
#         iteration = iteration + 1
#
#
# def test_conversion():
#     indicator = 'tele2_operator_tr_conversion'
#     global first_day_of_current_period
#     first_period = str(date_current.replace(day=1))
#     end_period = str(last_day_of_current_month)
#     operator_uuid = uuid_operator()
#     cur_naumen = connection.cursor()
#
#     cur_naumen.execute(
#         "select coalesce(sum(calls),0) from mv_user_calls_result_daily_n "
#         "where date_work between %s and %s "
#         "and partner_uuid = %s "
#         "and (result not in ('Автоответчик', 'Не берут трубку', 'Неправильный номер')) "
#         "and operator_uuid = %s",
#         (first_period, end_period, partner_uuid, operator_uuid,))
#     sum_calls_sql = cur_naumen.fetchone()[0]
#
#     cur_naumen.execute("select sum(calls) from mv_user_calls_result_daily_n  "
#                         "where date_work between %s and %s "
#                         "and operator_uuid = %s "
#                         "and partner_uuid = %s "
#                         "and result in ('Услуга подключена', 'Оффер. Услуга подключена')",
#                         (first_period, end_period, operator_uuid, partner_uuid,))
#     connected_services_sql = cur_naumen.fetchone()
#     if sum_calls_sql is None:
#         conversion_sql = 0
#     else:
#         conversion_sql = float(connected_services_sql[0] * 100 / sum_calls_sql)
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": end_period
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == conversion_sql
#
#
# def test_sales_widget_efficiency():
#     global first_day_of_current_period
#     first_period = str(date_current.replace(day=1))
#     end_period = str(last_day_of_current_month)
#     indicator = 'tele2_efficiency'
#     cur_naumen = connection.cursor()
#     cur = conn.cursor()
#     operator_uuid = uuid_operator()
#     cur_naumen.execute("select coalesce(sum(calls),0) from mv_user_calls_result_daily_n "
#                            "where date_work between %s and %s "
#                            "and partner_uuid = %s "
#                            "and result in ('Услуга подключена', 'Оффер. Услуга подключена') "
#                            "and operator_uuid = %s",
#                             (first_period, end_period, partner_uuid, operator_uuid,))
#     min_production_of_the_month_fact = cur_naumen.fetchone()
#
#     cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
#                        "where date_work between %s and %s "
#                        "and online_status = 'online' "
#                        "and partner_uuid = %s "
#                        "and away_status_reason is null "
#                        "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
#                        "and operator_uuid = %s ",
#                         (first_period, end_period, partner_uuid, operator_uuid,))
#     work_hours_of_the_month_fact = cur_naumen.fetchone()
#     if min_production_of_the_month_fact is None:
#         min_production_of_the_month_fact = 0
#     else:
#         min_production_of_the_month_fact = min_production_of_the_month_fact[0]
#     cur.execute("Select sum(value) from lcentrix_idle_hours where user_id = %s and date between %s and %s",
#                 (id_users(), first_period, end_period,))
#     penalty_hours = cur.fetchone()[0]
#     if penalty_hours is None:
#         penalty_hours = 0
#     if work_hours_of_the_month_fact is None or work_hours_of_the_month_fact == 0:
#         main_efficiency = 0
#     else:
#         work_hours_of_the_month_fact = (work_hours_of_the_month_fact[0] - penalty_hours) / 3600
#         main_efficiency = float(min_production_of_the_month_fact) / float(work_hours_of_the_month_fact)
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": end_period
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert round(requestdict['data'][indicator]['value'], 5) == round(main_efficiency, 5)
#
#
# def test_sales_widget_payment_level():
#     global first_day_of_current_period
#     first_period = str(date_current.replace(day=1))
#     end_period = str(last_day_of_current_month)
#     indicator = 'tele2_operator_payment_level'
#     cur_naumen = connection.cursor()
#     cur = conn.cursor()
#     operator_uuid = uuid_operator()
#
#     cur_naumen.execute("select coalesce(sum(calls),0) from mv_user_calls_result_daily_n "
#                            "where date_work between %s and %s "
#                            "and partner_uuid = %s "
#                            "and result in ('Услуга подключена', 'Оффер. Услуга подключена') "
#                            "and operator_uuid = %s",
#                             (first_period, end_period, partner_uuid, operator_uuid,))
#     min_production_of_the_month_fact = cur_naumen.fetchone()
#     if min_production_of_the_month_fact is None:
#         min_production_of_the_month_fact = 0
#     else:
#         min_production_of_the_month_fact = min_production_of_the_month_fact[0]
#
#     cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
#                        "where date_work between %s and %s "
#                        "and online_status = 'online' "
#                        "and partner_uuid = %s "
#                        "and away_status_reason is null "
#                        "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
#                        "and operator_uuid = %s ",
#                         (first_period, end_period, partner_uuid, operator_uuid,))
#     work_hours_of_the_month_fact = cur_naumen.fetchone()
#     if work_hours_of_the_month_fact is None:
#         work_hours_of_the_month_fact = 0
#     else:
#         work_hours_of_the_month_fact = work_hours_of_the_month_fact[0]
#
#     cur.execute("Select sum(value) from lcentrix_idle_hours where user_id = %s and date between %s and %s",
#                 (id_users(), first_period, end_period,))
#     penalty_hours = cur.fetchone()[0]
#     if penalty_hours is not None:
#         work_hours_of_the_month_fact = work_hours_of_the_month_fact - penalty_hours
#
#     if work_hours_of_the_month_fact == 0:
#         kpi = 0
#     else:
#         kpi = float(min_production_of_the_month_fact) / (float(work_hours_of_the_month_fact) / 3600)
#
#     # вычисление ставки за услуги
#     if kpi < 2:
#         rate_service = 0
#     elif kpi < 2.5 and operator_position_id() == 2:
#         rate_service = 0
#     elif kpi < 2.5 and operator_position_id() != 2:
#         rate_service = 15
#     elif kpi < 2.8:
#         rate_service = 25
#     elif kpi < 3.1:
#         rate_service = 30
#     elif kpi < 3.4:
#         rate_service = 34
#     elif kpi < 4:
#         rate_service = 40
#     elif kpi < 4.3:
#         rate_service = 43
#     else:
#         rate_service = 50
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": end_period
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == rate_service
#
#
# def test_sales_widget_connected_service():
#     global first_day_of_current_period
#     first_period = str(date_current.replace(day=1))
#     end_period = str(last_day_of_current_month)
#     indicator = 'tele2_operator_lcentrix_connected_services'
#     cur_naumen = connection.cursor()
#     operator_uuid = uuid_operator()
#
#     cur_naumen.execute("select coalesce(sum(calls),0) from mv_user_calls_result_daily_n "
#                            "where date_work between %s and %s "
#                            "and partner_uuid = %s "
#                            "and result in ('Услуга подключена', 'Оффер. Услуга подключена') "
#                            "and operator_uuid = %s",
#                             (first_period, end_period, partner_uuid, operator_uuid,))
#     min_production_of_the_month_fact = cur_naumen.fetchone()
#     if min_production_of_the_month_fact is None:
#         min_production_of_the_month_fact = 0
#     else:
#         min_production_of_the_month_fact = min_production_of_the_month_fact[0]
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": end_period
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == min_production_of_the_month_fact
#
#
# def test_sales_widget_next_level():
#     global first_day_of_current_period
#     first_period = str(date_current.replace(day=1))
#     end_period = str(last_day_of_current_month)
#     indicator = 'services_quantity_for_next_level'
#     cur_naumen = connection.cursor()
#     cur = conn.cursor()
#     operator_uuid = uuid_operator()
#     yesterday = str(date_current - datetime.timedelta(days=1))
#
#     # Подключки факт за период
#     cur_naumen.execute("select coalesce(sum(calls),0) from mv_user_calls_result_daily_n "
#                            "where date_work between %s and %s "
#                            "and partner_uuid = %s "
#                            "and result in ('Услуга подключена', 'Оффер. Услуга подключена') "
#                            "and operator_uuid = %s",
#                             (first_period, end_period, partner_uuid, operator_uuid,))
#     min_production_of_the_month_fact = cur_naumen.fetchone()
#     if min_production_of_the_month_fact is None:
#         min_production_of_the_month_fact = 0
#     else:
#         min_production_of_the_month_fact = float(min_production_of_the_month_fact[0])
#
#     # Рабочее время за период 1 - вчерашний день
#     cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
#                        "where date_work between %s and %s "
#                        "and online_status = 'online' "
#                        "and partner_uuid = %s "
#                        "and away_status_reason is null "
#                        "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
#                        "and operator_uuid = %s ",
#                         (first_period, yesterday, partner_uuid, operator_uuid,))
#     work_hours_of_the_month_fact = cur_naumen.fetchone()
#     cur.execute("Select sum(value) from lcentrix_idle_hours where user_id = %s and date between %s and %s",
#                 (id_users(), first_period, end_period,))
#     penalty_hours = cur.fetchone()[0]
#     if penalty_hours is None:
#         penalty_hours = 0
#     if work_hours_of_the_month_fact is None:
#         work_hours_of_the_month_fact = 0
#     else:
#         work_hours_of_the_month_fact = work_hours_of_the_month_fact[0] - penalty_hours
#
#     # Рабочее время ЗА сегодняшее число план
#     indicator_id = 1990
#     cur.execute('select sum(value::int)  from plan_objects_indicators join '
#                 'objects_indicators on plan_objects_indicators.object_indicator_id = objects_indicators.id '
#                 'join users on objects_indicators.object_id = users.id '
#                 'where period_begin = %s '
#                 'and object_id = %s'
#                 'and objects_indicators.indicator_id = %s',
#                 (date_current, id_users(), indicator_id,))
#     work_hours_current_day_plan = cur.fetchone()[0]
#     if work_hours_current_day_plan is None:
#         work_hours_current_day_plan = 0
#
#     # Общее рабочее время
#     total_work_hours = float((work_hours_of_the_month_fact + work_hours_current_day_plan) / 3600)
#
#     # вычисление ставки за услуги
#     cur_naumen.execute("select sum(duration) from mv_user_status_period_daily_n "
#                            "where date_work between %s and %s "
#                            "and online_status = 'online' "
#                            "and partner_uuid = %s "
#                            "and away_status_reason is null "
#                            "and (speaking = 'speaking' or wrapup = 'wrapup' or ringing = 'ringing' or normal = 'normal') "
#                            "and operator_uuid = %s ",
#                             (first_period, end_period, partner_uuid, operator_uuid,))
#     work_hours_of_the_month_fact = cur_naumen.fetchone()
#     cur.execute("Select sum(value) from lcentrix_idle_hours where user_id = %s and date between %s and %s",
#                 (id_users(), first_period, end_period,))
#     penalty_hours = cur.fetchone()
#     if penalty_hours[0] is None:
#         penalty_hours = 0
#     else:
#         penalty_hours = penalty_hours[0]
#
#     if work_hours_of_the_month_fact is None:
#         work_hours_of_the_month_fact = 0
#     else:
#         work_hours_of_the_month_fact = float((work_hours_of_the_month_fact[0] - penalty_hours) / 3600)
#
#     if work_hours_of_the_month_fact == 0:
#         kpi = 0
#     else:
#         kpi = float(min_production_of_the_month_fact / work_hours_of_the_month_fact)
#
#     # вычисление недостающей эффективности для стажера:
#     efficiency = 0
#     if operator_position_id() != 2:
#         if kpi < 2:
#             efficiency = 2
#         elif kpi < 2.5:
#             efficiency = 2.5
#         elif kpi < 2.8:
#             efficiency = 2.8
#         elif kpi < 3.1:
#             efficiency = 3.1
#         elif kpi < 3.4:
#             efficiency = 3.4
#         elif kpi < 4:
#             efficiency = 4
#         elif kpi < 4.3:
#             efficiency = 4.3
#         elif kpi >= 4.3:
#             efficiency = 'max'
#     else:
#         if kpi < 2.5:
#             efficiency = 2.5
#         elif kpi < 2.8:
#             efficiency = 2.8
#         elif kpi < 3.1:
#             efficiency = 3.1
#         elif kpi < 3.4:
#             efficiency = 3.4
#         elif kpi < 4:
#             efficiency = 4
#         elif kpi < 4.3:
#             efficiency = 4.3
#         elif kpi >= 4.3:
#             efficiency = 'max'
#
#     # next_level = efficiency * total_work_hours - min_production_of_the_month_fact
#     if efficiency == 'max':
#         next_level_round = 'max'
#     else:
#         next_level = efficiency * total_work_hours - min_production_of_the_month_fact
#         next_level_round = math.ceil(next_level)
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users(),
#             "object_type": 'user',
#             "parameters": {
#                 "organization_id": organization_id,
#                 "period_begin": first_period,
#                 "period_end": end_period
#             }
#     }
#
#     response = requests.post(url, json=body, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     requestdict = json.loads(response.content)
#     assert requestdict['data'][indicator]['value'] == next_level_round


# закрытие подключения к бд по ssh
def test_stopserver():
    stop(conn, conn_node, server)
    stop_naumen(connection)
    stop_oracle(connection_ora)
