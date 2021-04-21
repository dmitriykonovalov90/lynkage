import btoken
import requests
import json
import db
import datetime
import db_naumen
import math

global conn, conn_node, server, connection

start = db.startdb
stop = db.stopdb
start_naumen = db_naumen.start_db_naumen
stop_naumen = db_naumen.stop_db_naumen


# данные для выполнения теста
login = 'ta.yakobson1'
client_id = 2
client_secret = '23IzWSgkX5MUlpxSAYJr2o1sM8DRkLXI7vlZFExW'
grant_type = 'password'
organization = 'lcentrix'
password = "iWAQtZMw"
headers = btoken.get_token(login, password, client_secret, grant_type, client_id, organization)

salegroup_id = 101
organization_id = 1
partner_uuid = 'corebo00000000000lt6e97a8cauvg80'
object_type = 'user'
#url = 'https://api.uat.navigator.lynkage.ru/formulaConstructor/get'


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
    global conn, conn_node, server, connection
    conn, conn_node, server = start()
    connection = start_naumen()



# вычленение id пользователя по имеющемуся логину
def id_users():
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE login = %s", (login,))
    id_user = cur.fetchone()[0]
    return id_user


def uuid_operator():
    cur = conn.cursor()
    user_id = id_users()
    cur.execute('select etl_user_id from users '
                'where id = %s',
                (user_id,))
    uuid = cur.fetchone()
    if uuid is None:
        uuid = 0
    else:
        uuid = uuid[0]
    return uuid


def operator_position_id():
    cur = conn.cursor()
    cur.execute("select position_id from users_positions "
                "where user_id = %s",
                (id_users(),))
    position_id = cur.fetchone()
    if position_id is None:
        position_id = 2
        return position_id
    else:
        return position_id[0]


def test_login():
    url = 'https://api.uat.navigator.lynkage.ru/login'
    body = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": grant_type,
            "organization": organization,
            "password": password,
            "username": login
            }

    response = requests.post(url, json=body)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    requestdict = json.loads(response.content)
    assert(requestdict['data']['access_token'] is not None)
    assert(requestdict['data']['user_info']['login'] == login)


def test_get_roles_and_organizations():
    url = 'https://node.uat.navigator.lynkage.ru/roles/arms/get'
    cur = conn.cursor()
    cur.execute("Select role_id from users_roles where user_id = %s",
                (id_users(),))
    id_roles = cur.fetchone()
    assert id_roles is not None
    id_roles = id_roles[0]
    body = {
        "role_ids": [id_roles],
        "organization_id": organization_id}
    response = requests.post(url, json=body)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    requestdict = json.loads(response.content)
    assert requestdict['data'][0]['id'] == id_roles


def test_get_arm_modules():
    url = 'https://node.uat.navigator.lynkage.ru/arms/modules/get'

    body = {
        "arm": "brigadir",
        "organization_id": organization_id
        }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"


def test_get_sub_arm_modules():
    url = 'https://node.uat.navigator.lynkage.ru/modules/widgets/get'

    body = {
        "sub_arm": "brigadir",
        "organization_id": organization_id,
        "module": "main"
        }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert len(requestdict['data']) == 11


def test_content_work_time_graph_tele2():
    url = 'https://node.uat.navigator.lynkage.ru/widgets/content'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "data_level": "rgp",
        "user_id": id_users(),
        "widget_acronim": "widget_working_time_graph_tele2",
        "parameters":
            {
             "arm": "brigadir",
             "object_id": salegroup_id,
             "period_begin": first_period,
             "period_end": end_period
            }
    }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['data']['graphic']['acronim'] == 'graphic'


def test_content_efficiency_graph_tele2():
    url = 'https://node.uat.navigator.lynkage.ru/widgets/content'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "data_level": "rgp",
        "user_id": id_users(),
        "widget_acronim": "widget_efficiency_graph_tele2",
        "parameters":
            {
             "arm": "brigadir",
             "object_id": salegroup_id,
             "period_begin": first_period,
             "period_end": end_period
            }
    }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['data']['graphic']['acronim'] == 'graphic'


def test_content_efficiency_multi_tele2():
    url = 'https://node.uat.navigator.lynkage.ru/widgets/content'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "data_level": "rgp",
        "user_id": id_users(),
        "widget_acronim": "widget_efficiency_multi_tele2",
        "parameters":
            {
             "arm": "brigadir",
             "object_id": salegroup_id,
             "period_begin": first_period,
             "period_end": end_period
            }
    }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['data']['applications_hour']['acronim'] == 'applications_hour'


def test_content_revenue_forecast_tele2():
    url = 'https://node.uat.navigator.lynkage.ru/widgets/content'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "data_level": "rgp",
        "user_id": id_users(),
        "widget_acronim": "widget_revenue_forecast_tele2_brigadir",
        "parameters":
            {
             "arm": "brigadir",
             "object_id": salegroup_id,
             "period_begin": first_period,
             "period_end": end_period
            }
    }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    requestdict = json.loads(response.content)
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['data']['payment']['acronim'] == 'payment'


def test_content_working_hours_in_line_tele2():
    url = 'https://node.uat.navigator.lynkage.ru/widgets/content'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "data_level": "rgp",
        "user_id": id_users(),
        "widget_acronim": "widget_working_hours_in_line_tele2",
        "parameters":
            {
             "arm": "brigadir",
             "object_id": salegroup_id,
             "period_begin": first_period,
             "period_end": end_period
            }
    }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['data']['current']['acronim'] == 'current'


def test_content_working_hours_today_tele2():
    url = 'https://node.uat.navigator.lynkage.ru/widgets/content'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "data_level": "rgp",
        "user_id": id_users(),
        "widget_acronim": "widget_working_hours_today_tele2",
        "parameters":
            {
             "arm": "brigadir",
             "object_id": salegroup_id,
             "period_begin": first_period,
             "period_end": end_period
            }
    }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['data']['current']['acronim'] == 'current'


def test_content_sales_small_tele2():
    url = 'https://node.uat.navigator.lynkage.ru/widgets/content'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "data_level": "rgp",
        "user_id": id_users(),
        "widget_acronim": "widget_sales_small_tele2",
        "parameters":
            {
             "arm": "brigadir",
             "object_id": salegroup_id,
             "period_begin": first_period,
             "period_end": end_period
            }
    }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['data']['current']['acronim'] == 'current'


def test_content_sales_today_tele2():
    url = 'https://node.uat.navigator.lynkage.ru/widgets/content'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "data_level": "rgp",
        "user_id": id_users(),
        "widget_acronim": "widget_sales_today_tele2",
        "parameters":
            {
             "arm": "brigadir",
             "object_id": salegroup_id,
             "period_begin": first_period,
             "period_end": end_period
            }
    }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['data']['current']['acronim'] == 'current'


def test_content_aht_tele2():
    url = 'https://node.uat.navigator.lynkage.ru/widgets/content'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "data_level": "rgp",
        "user_id": id_users(),
        "widget_acronim": "widget_ant_tele2",
        "parameters":
            {
             "arm": "brigadir",
             "object_id": salegroup_id,
             "period_begin": first_period,
             "period_end": end_period
            }
    }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['data']['current']['acronim'] == 'current'


def test_content_working_hours_this_day_tele2():
    url = 'https://node.uat.navigator.lynkage.ru/widgets/content'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "data_level": "rgp",
        "user_id": id_users(),
        "widget_acronim": "widget_working_hours_this_day_tele2",
        "parameters":
            {
             "arm": "brigadir",
             "object_id": salegroup_id,
             "period_begin": first_period,
             "period_end": end_period
            }
    }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['data']['current']['acronim'] == 'current'


def test_content_sales_this_day_tele2():
    url = 'https://node.uat.navigator.lynkage.ru/widgets/content'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "data_level": "rgp",
        "user_id": id_users(),
        "widget_acronim": "widget_sales_this_day_tele2",
        "parameters":
            {
             "arm": "brigadir",
             "object_id": salegroup_id,
             "period_begin": first_period,
             "period_end": end_period
            }
    }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['data']['current']['acronim'] == 'current'


def test_get_operational_reports():
    url = 'https://test.navigator.lynkage.ru/main/reports/operational-report'
    response = requests.get(url)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"


def test_get_organizations():
    url = 'https://api.uat.navigator.lynkage.ru/organizations/get'

    body = {
        "filter":
            {
                "active": True
            }
    }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['data'][0]['acronim'] == 'lcentrix'


def test_get_module_operational_report():
    url = 'https://node.uat.navigator.lynkage.ru/modules/widgets/get'

    body = {
        "organization_id": organization_id,
        "sub_arm": "brigadir",
        "module": "operational_report"
    }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['data'][0]['acronim'] == 'widget_report_operators'


def test_content_widget_report_operators():
    url = 'https://node.uat.navigator.lynkage.ru/widgets/content'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "data_level": "head_org",
        "user_id": id_users(),
        "widget_acronim": "widget_report_operators",
        "parameters": {
         "arm": "brigadir",
         "object_id": salegroup_id,
         "period_begin": first_period,
         "period_end": end_period
        }
     }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['data']['body'][0]['fio']['name'] == 'ФИО'


def test_get_widget_report_operators_work():
    url = 'https://node.uat.navigator.lynkage.ru/modules/widgets/get'

    body = {
        "organization_id": organization_id,
        "sub_arm": "brigadir",
        "module": "report_operators_work"
     }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['data'][0]['acronim'] == 'widget_report_operators_work'


def test_content_widget_report_operators_work():
    url = 'https://node.uat.navigator.lynkage.ru/widgets/content'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "data_level": "rgp",
        "user_id": id_users(),
        "widget_acronim": "widget_report_operators_work",
        "parameters": {
         "arm": "brigadir",
         "object_id": salegroup_id,
         "period_begin": first_period,
         "period_end": end_period
        }
     }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['status']['message'] == 'Success'


def test_get_pay_sheet():
    url = 'https://node.uat.navigator.lynkage.ru/modules/widgets/get'

    body = {
        "organization_id": organization_id,
        "sub_arm": "brigadir",
        "module": "pay_sheet"
     }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['status']['message'] == 'Success'


def test_content_widget_pay_sheet():
    url = 'https://node.uat.navigator.lynkage.ru/widgets/content'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "data_level": "rgp",
        "user_id": id_users(),
        "widget_acronim": "widget_pay_sheet",
        "parameters": {
         "arm": "brigadir",
         "object_id": salegroup_id,
         "period_begin": first_period,
         "period_end": end_period
        }
     }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['status']['message'] == 'Success'


def test_get_working_hours_schedule():
    url = 'https://node.uat.navigator.lynkage.ru/modules/widgets/get'

    body = {
        "organization_id": organization_id,
        "sub_arm": "brigadir",
        "module": "working_hours_schedule"
     }

    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['status']['message'] == 'Success'


def test_content_widget_working_hours_schedule():
    url = 'https://node.uat.navigator.lynkage.ru/widgets/content'
    first_period = str(first_day_of_current_period)
    end_period = str(last_day_of_current_month)

    body = {
        "data_level": "rgp",
        "user_id": id_users(),
        "widget_acronim": "widget_working_time_schedules",
        "parameters": {
         "arm": "brigadir",
         "object_id": salegroup_id,
         "period_begin": first_period,
         "period_end": end_period
        }
     }
    response = requests.post(url, json=body, headers=headers)
    assert response.status_code != 500, "internal server error"
    assert response.status_code != 405, "Ошибка метода отправки"
    assert response.status_code != 404, "Ошибка авторизации"
    assert response.status_code == 200, "Ошибка"
    requestdict = json.loads(response.content)
    assert requestdict['status']['message'] == 'Success'


# def test_get_personal_info():
#     url = 'https://api.uat.navigator.lynkage.ru/salegroups/get'
#
#     response = requests.get(url, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     assert response.status_code != 404, "Ошибка авторизации"
#     assert response.status_code == 200, "Ошибка"
#     requestdict = json.loads(response.content)
#     print(requestdict)
#     result_file = open('test.txt', 'w')
#     result_file.write(str(requestdict))
#     result_file.close()
#
#
# def test_get_personal_info2():
#     url = 'https://api.uat.navigator.lynkage.ru/users/getForemen'
#
#     response = requests.get(url, headers=headers)
#     assert response.status_code != 500, "internal server error"
#     assert response.status_code != 405, "Ошибка метода отправки"
#     assert response.status_code != 404, "Ошибка авторизации"
#     assert response.status_code == 200, "Ошибка"
#     requestdict = json.loads(response.content)
#     print(requestdict)
#     # result_file = open('test.txt', 'w')
#     # result_file.write(str(requestdict))
#     # result_file.close()



# закрытие подключения к бд по ssh
def test_stopserver():
    stop(conn, conn_node, server)
    stop_naumen(connection)

