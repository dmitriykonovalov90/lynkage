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

login = '58080013_volga'
login_non_volga = '58080013'
password = "YhgxFoLW"
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


""" Если нужна другая дата, то вот """

# date_current_oracle = 202104
# first_period = str('2021-04-01')
# end_period = str('2021-04-30')

# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО ЗАЯВОК ФАКТ                       '''
#
#
# def test_operator_requests_created(request_created, id_users):
#     indicator = 'rtk_volga_requests_created'
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users,
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
#     print('\u001B[33msql:\u001B[0m', request_created)
#     assert requestdict['data'][indicator]['value'] == request_created
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО ЗАЯВОК ПЛАН                      '''
#
#
# def test_operator_requests_plan(request_plan, id_users):
#     indicator = 'rtk_volga_requests_created_plan'
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users,
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
#     print('\u001B[33msql:\u001B[0m', request_plan)
#     assert requestdict['data'][indicator]['value'] == request_plan
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО ЗАЯВОК ПРОЦЕНТЫ                     '''
#
#
# def test_operator_requests_percent(request_created, request_plan, id_users):
#     indicator = 'rtk_volga_requests_created_plan_percent'
#
#     if request_plan is None:
#         result_sql = 0
#     else:
#         result_sql = float(request_created) * 100 / float(request_plan)
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users,
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
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО ПОДКЛЮЧЕННЫХ УСЛУГ ФАКТ                       '''
#
#
# def test_operator_connected_all_services(connected_all_services, id_users):
#     indicator = 'rtk_volga_connected_services_count'
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users,
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
#     print('\n\u001B[36mВиджет "Количество подключенных услуг", факт')
#     print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
#     print('\u001B[33msql:\u001B[0m', connected_all_services)
#     assert requestdict['data'][indicator]['value'] == connected_all_services
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО ПОДКЛЮЧЕННЫХ УСЛУГ ПЛАН                       '''
#
#
# def test_operator_connect_services_plan(connect_services_plan, id_users):
#     indicator = 'connected_all_services_count_plan'
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users,
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
#     print('\n\n\u001B[36mВиджет "Количество подключенных услуг", план')
#     print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
#     print('\u001B[33msql:\u001B[0m', connect_services_plan)
#     assert requestdict['data'][indicator]['value'] == connect_services_plan
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО ПОДКЛЮЧЕННЫХ УСЛУГ ПРОЦЕНТ                       '''
#
#
# def test_operator_all_connect_services_percent(connected_all_services, connect_services_plan, id_users):
#     indicator = 'rtk_volga_connected_services_count_plan_percent'
#
#     if connect_services_plan is None:
#         result_sql = 0
#     else:
#         result_sql = float(connected_all_services) * 100 / float(connect_services_plan)
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users,
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
#     print('\n\n\u001B[36mВиджет "Количество подключенных услуг", процент выполнения')
#     print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
#     print('\u001B[33msql:\u001B[0m', result_sql)
#     assert round(requestdict['data'][indicator]['value'], 5) == round(result_sql, 5)
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО КОНТАКТОВ ФАКТ                       '''
#
#
# def test_operator_contacts_count(contacts_count, id_users):
#     indicator = 'rtk_volga_contacts_count'
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users,
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
#
#     print('\n\u001B[36mВиджет "Количество контактов", факт')
#     print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
#     print('\u001B[33msql:\u001B[0m', contacts_count)
#     assert requestdict['data'][indicator]['value'] == contacts_count
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО КОНТАКТОВ ПЛАН                       '''
#
#
# def test_operator_contacts_count_plan(contacts_count_plan, id_users):
#     indicator = 'rtk_volga_contacts_count_plan'
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users,
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
#     print('\n\n\u001B[36mВиджет "Количество контактов", план')
#     print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
#     print('\u001B[33msql:\u001B[0m', contacts_count_plan)
#     assert requestdict['data'][indicator]['value'] == contacts_count_plan
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОЛИЧЕСТВО КОНТАКТОВ ПРОЦЕНТ                       '''
#
#
# def test_operator_contacts_count_percent(contacts_count, contacts_count_plan, id_users):
#     indicator = 'rtk_volga_contacts_count_plan_percent'
#
#     if contacts_count_plan is None:
#         result_sql = 0
#     else:
#         result_sql = float(contacts_count) * 100 / float(contacts_count_plan)
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users,
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
#     print('\n\n\u001B[36mВиджет "Количество контактов", процент выполнения')
#     print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
#     print('\u001B[33msql:\u001B[0m', result_sql)
#     assert round(requestdict['data'][indicator]['value'], 5) == round(result_sql, 5)
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - РАБОЧИЕ ЧАСЫ ФАКТ                       '''
#
#
# def test_operator_work_hours(work_hours, id_users):
#     indicator = 'rtk_volga_work_hours'
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users,
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
#
#     print('\n\u001B[36mВиджет "Рабочие часы", факт')
#     print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
#     print('\u001B[33msql:\u001B[0m', work_hours, '. В часах - ', work_hours/3600)
#     assert requestdict['data'][indicator]['value'] == work_hours
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - РАБОЧИЕ ЧАСЫ ПЛАН                       '''
#
#
# def test_operator_work_hours_plan(work_hours_plan, id_users):
#     indicator = 'rtk_volga_work_hours_plan'
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users,
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
#     print('\n\n\u001B[36mВиджет "Рабочие часы", план')
#     print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
#     print('\u001B[33msql:\u001B[0m', work_hours_plan, '. В часах - ', work_hours_plan/3600)
#     assert requestdict['data'][indicator]['value'] == work_hours_plan
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - РАБОЧИЕ ЧАСЫ ПРОЦЕНТ                       '''
#
#
# def test_operator_work_hours_percent(work_hours, work_hours_plan, id_users):
#     indicator = 'rtk_volga_work_hours_plan_percent'
#
#     if work_hours_plan is None:
#         result_sql = 0
#     else:
#         result_sql = float(work_hours) * 100 / float(work_hours_plan)
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users,
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
#     print('\n\n\u001B[36mВиджет "Рабочие часы", процент выполнения')
#     print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
#     print('\u001B[33msql:\u001B[0m', result_sql)
#     assert round(requestdict['data'][indicator]['value'], 5) == round(result_sql, 5)
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОНВЕРСИЯ ИЗ КОНТАКТОВ В ЗАЯВКИ                       '''
#
#
# def test_operator_contacts_to_requests_conversion(contacts_count, request_created, id_users):
#     indicator = 'rtk_volga_contacts_to_requests_conversion'
#
#     if contacts_count is None:
#         result_sql = 0
#     else:
#         result_sql = request_created * 100 / contacts_count
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users,
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
#     print('\n\n\u001B[36mВиджет "Конверсия из контактов в заявки"')
#     print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
#     print('\u001B[33msql:\u001B[0m', result_sql)
#     assert round(requestdict['data'][indicator]['value'], 5) == round(float(result_sql), 5)
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОНВЕРСИЯ ИЗ КОНТАКТОВ В ДОГОВОРЫ                       '''
#
#
# def test_operator_contacts_to_connected_conversion(connected_all_services, contacts_count, id_users):
#     indicator = 'rtk_volga_contacts_to_connected_conversion'
#
#     if contacts_count is None:
#         result = 0
#     else:
#         result = connected_all_services * 100 / contacts_count
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users,
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
#     print('\n\n\u001B[36mВиджет "Конверсия из контактов в договоры"')
#     print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
#     print('\u001B[33msql:\u001B[0m', result)
#     assert round(requestdict['data'][indicator]['value'], 5) == round(float(result), 5)
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - КОНВЕРСИЯ ИЗ ЗАЯВОК В ДОГОВОРЫ                       '''
#
#
# def test_operator_requests_to_connected_conversion(connected_all_services, request_created, id_users):
#     indicator = 'rtk_volga_requests_to_connected_conversion'
#
#     if request_created == 0:
#         result = 0
#     else:
#         result = connected_all_services * 100 / request_created
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users,
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
#     print('\n\n\u001B[36mВиджет "Конверсия из заявок в договоры"')
#     print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
#     print('\u001B[33msql:\u001B[0m', result)
#     assert round(requestdict['data'][indicator]['value'], 5) == round(float(result), 5)
#
#
# '''                    КРАТКИЙ ВЕРХНИЙ ВИДЖЕТ - "ЭФФЕКТИВНОСТЬ"                       '''
#
#
# def test_operator_efficiency(contacts_count, work_hours, id_users):
#     indicator = 'rtk_volga_efficiency'
#
#     if work_hours is None:
#         result = 0
#     else:
#         result = contacts_count / (work_hours / 3600)
#
#     body = {
#             "indicator_acronim": indicator,
#             "object_id": id_users,
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
#     print('\n\n\u001B[36mВиджет "Эффективность"')
#     print('\u001B[33mapi:\u001B[0m', requestdict['data'][indicator]['value'])
#     print('\u001B[33msql:\u001B[0m', result)
#     assert round(requestdict['data'][indicator]['value'], 5) == round(float(result), 5)


def test_operator_connected_services_types_graph(id_users, connected_services_types_graph):
    indicator = 'rtk_volga_operator_connected_services_types_graph'

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
    name_indexer = dict((p['name'], i) for i, p in enumerate(requestdict['data'][indicator]['value']))
    print('\n\n\u001B[36mГрафический Виджет "Все подключенные услуги - факты"')

    """ ШПД """
    assert requestdict['data'][indicator]['value'][name_indexer.get('ШПД')]['count'] == \
           connected_services_types_graph['shpd']
    print('\u001B[33mШПД api:\u001B[0m',
          requestdict['data'][indicator]['value'][name_indexer.get('ШПД')]['count'])
    print('\u001B[33mШПД sql:\u001B[0m', connected_services_types_graph['shpd'])

    """ Гарантия """
    assert requestdict['data'][indicator]['value'][name_indexer.get('Гарантия')]['count'] == \
           connected_services_types_graph['dgo']
    print('\u001B[33mГарантия api:\u001B[0m',
          requestdict['data'][indicator]['value'][name_indexer.get('Гарантия')]['count'])
    print('\u001B[33mГарантия sql:\u001B[0m', connected_services_types_graph['dgo'])

    """ ИТВ """
    assert requestdict['data'][indicator]['value'][name_indexer.get('ИТВ')]['count'] == \
           connected_services_types_graph['iptv']
    print('\u001B[33mИТВ api:\u001B[0m',
          requestdict['data'][indicator]['value'][name_indexer.get('ИТВ')]['count'])
    print('\u001B[33mИТВ sql:\u001B[0m', connected_services_types_graph['iptv'])

    """ Маруся """
    assert requestdict['data'][indicator]['value'][name_indexer.get('Маруся')]['count'] == \
           connected_services_types_graph['equip_smartspeaker']
    print('\u001B[33mМаруся api:\u001B[0m',
          requestdict['data'][indicator]['value'][name_indexer.get('Маруся')]['count'])
    print('\u001B[33mМаруся sql:\u001B[0m', connected_services_types_graph['equip_smartspeaker'])

    """ МВНО """
    assert requestdict['data'][indicator]['value'][name_indexer.get('МВНО')]['count'] == \
           connected_services_types_graph['mvno']
    print('\u001B[33mМВНО api:\u001B[0m',
          requestdict['data'][indicator]['value'][name_indexer.get('МВНО')]['count'])
    print('\u001B[33mМВНО sql:\u001B[0m', connected_services_types_graph['mvno'])

    """ ОТА """
    assert requestdict['data'][indicator]['value'][name_indexer.get('ОТА')]['count'] == \
           connected_services_types_graph['ota']
    print('\u001B[33mОТА api:\u001B[0m',
          requestdict['data'][indicator]['value'][name_indexer.get('ОТА')]['count'])
    print('\u001B[33mОТА sql:\u001B[0m', connected_services_types_graph['ota'])

    """ Винк+ """
    assert requestdict['data'][indicator]['value'][name_indexer.get('Винк+')]['count'] == \
           connected_services_types_graph['equip_andrstb']
    print('\u001B[33mВинк+ api:\u001B[0m',
          requestdict['data'][indicator]['value'][name_indexer.get('Винк+')]['count'])
    print('\u001B[33mВинк+ sql:\u001B[0m', connected_services_types_graph['equip_andrstb'])

    """ ВН """
    assert requestdict['data'][indicator]['value'][name_indexer.get('ВН')]['count'] == \
           connected_services_types_graph['equip_video']
    print('\u001B[33mВН api:\u001B[0m',
          requestdict['data'][indicator]['value'][name_indexer.get('ВН')]['count'])
    print('\u001B[33mВН sql:\u001B[0m', connected_services_types_graph['equip_video'])


def test_operator_connected_services_types_graph_plan(id_users, connected_services_types_graph_plan):
    indicator = 'rtk_volga_operator_connected_services_types_graph'

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
    name_indexer = dict((p['name'], i) for i, p in enumerate(requestdict['data'][indicator]['value']))
    print('\n\n\u001B[36mГрафический Виджет "Все подключенные услуги - Планы"')

    """ ШПД """
    assert requestdict['data'][indicator]['value'][name_indexer.get('ШПД')]['plan'] == \
           connected_services_types_graph_plan['shpd']
    print('\u001B[33mШПД api:\u001B[0m',
          requestdict['data'][indicator]['value'][name_indexer.get('ШПД')]['plan'])
    print('\u001B[33mШПД sql:\u001B[0m', connected_services_types_graph_plan['shpd'])

    """ Гарантия """
    assert requestdict['data'][indicator]['value'][name_indexer.get('Гарантия')]['plan'] == \
           connected_services_types_graph_plan['dgo']
    print('\u001B[33mГарантия api:\u001B[0m',
          requestdict['data'][indicator]['value'][name_indexer.get('Гарантия')]['plan'])
    print('\u001B[33mГарантия sql:\u001B[0m', connected_services_types_graph_plan['dgo'])

    """ ИТВ """
    assert requestdict['data'][indicator]['value'][name_indexer.get('ИТВ')]['plan'] == \
           connected_services_types_graph_plan['iptv']
    print('\u001B[33mИТВ api:\u001B[0m',
          requestdict['data'][indicator]['value'][name_indexer.get('ИТВ')]['plan'])
    print('\u001B[33mИТВ sql:\u001B[0m', connected_services_types_graph_plan['iptv'])

    """ Маруся """
    assert requestdict['data'][indicator]['value'][name_indexer.get('Маруся')]['plan'] == \
           connected_services_types_graph_plan['equip_smartspeaker']
    print('\u001B[33mМаруся api:\u001B[0m',
          requestdict['data'][indicator]['value'][name_indexer.get('Маруся')]['plan'])
    print('\u001B[33mМаруся sql:\u001B[0m', connected_services_types_graph_plan['equip_smartspeaker'])

    """ МВНО """
    assert requestdict['data'][indicator]['value'][name_indexer.get('МВНО')]['plan'] == \
           connected_services_types_graph_plan['mvno']
    print('\u001B[33mМВНО api:\u001B[0m',
          requestdict['data'][indicator]['value'][name_indexer.get('МВНО')]['plan'])
    print('\u001B[33mМВНО sql:\u001B[0m', connected_services_types_graph_plan['mvno'])

    """ ОТА """
    assert requestdict['data'][indicator]['value'][name_indexer.get('ОТА')]['plan'] == \
           connected_services_types_graph_plan['ota']
    print('\u001B[33mОТА api:\u001B[0m',
          requestdict['data'][indicator]['value'][name_indexer.get('ОТА')]['plan'])
    print('\u001B[33mОТА sql:\u001B[0m', connected_services_types_graph_plan['ota'])

    """ Винк+ """
    assert requestdict['data'][indicator]['value'][name_indexer.get('Винк+')]['plan'] == \
           connected_services_types_graph_plan['equip_andrstb']
    print('\u001B[33mВинк+ api:\u001B[0m',
          requestdict['data'][indicator]['value'][name_indexer.get('Винк+')]['plan'])
    print('\u001B[33mВинк+ sql:\u001B[0m', connected_services_types_graph_plan['equip_andrstb'])

    """ ВН """
    assert requestdict['data'][indicator]['value'][name_indexer.get('ВН')]['plan'] == \
           connected_services_types_graph_plan['equip_video']
    print('\u001B[33mВН api:\u001B[0m',
          requestdict['data'][indicator]['value'][name_indexer.get('ВН')]['plan'])
    print('\u001B[33mВН sql:\u001B[0m', connected_services_types_graph_plan['equip_video'])
