import pytest
import btoken
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

login = '58020073_volga'
login_non_volga = '58020073'
password = "nFXAB5Q0"
client_id = 2
client_secret = '23IzWSgkX5MUlpxSAYJr2o1sM8DRkLXI7vlZFExW'
grant_type = 'password'
organization = 'rtk'

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


""" Если вдруг нужно за другую дату, то вот """

# date_current_oracle = 202104
# first_period = str('2021-04-01')
# end_period = str('2021-04-30')


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
def contacts_count_plan(id_users):
    cur = conn.cursor()
    indicator_id = 2879

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


""" ********************************* РАБОЧИЕ ЧАСЫ ФАКТ ************************************** """


@pytest.fixture()
def work_hours():
    cur_naumen = connection.cursor()

    cur_naumen.execute("""
    select sum(duration) from mv_user_status_full_daily
    where login = %s
    and date_work between %s and %s
    and main_status in ('available', 'notavailable')
     """, (login_non_volga, first_period, end_period,))

    result_sql = cur_naumen.fetchone()[0]

    if result_sql is None:
        result_sql = 0
    return result_sql


""" ********************************* РАБОЧИЕ ЧАСЫ ПЛАН ************************************** """


@pytest.fixture()
def work_hours_plan(id_users):
    cur = conn.cursor()
    indicator_id = 2941

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


""" ********************************* ОДИНОЧНЫЕ ПОДКЛЮЧЕННЫЕ УСЛУГИ ФАКТ ************************************** """


@pytest.fixture()
def connected_services_types_graph(fio_users):
    cur_ora_rtk = connection_ora.cursor()
    result = dict()

    ''' Количество услуг ШПД '''
    cur_ora_rtk.execute("""
    select sum(CNT_SHPD) from AGP_V_UNITED_REPORT_RES_LN where AGENT = (:1) and PERIOD = (:1)
     """, [fio_users, date_current_oracle])
    result_shpd = cur_ora_rtk.fetchone()[0]
    if result_shpd is None:
        result_shpd = 0
    result['shpd'] = result_shpd

    """ Количество услуг Гарантия """
    cur_ora_rtk.execute("""
    select sum(CNT_DGO) from AGP_V_UNITED_REPORT_RES_LN where AGENT = (:1) and PERIOD = (:1)
     """, [fio_users, date_current_oracle])
    result_dgo = cur_ora_rtk.fetchone()[0]
    if result_dgo is None:
        result_dgo = 0
    result['dgo'] = result_dgo

    """ Количество услуг ИТВ """
    cur_ora_rtk.execute("""
    select sum(CNT_IPTV) from AGP_V_UNITED_REPORT_RES_LN where AGENT = (:1) and PERIOD = (:1)
     """, [fio_users, date_current_oracle])
    result_iptv = cur_ora_rtk.fetchone()[0]
    if result_iptv is None:
        result_iptv = 0
    result['iptv'] = result_iptv

    """ Количество услуг Маруся """
    cur_ora_rtk.execute("""
    select sum(CNT_EQUIP_SMARTSPEAKER) from AGP_V_UNITED_REPORT_RES_LN where AGENT = (:1) and PERIOD = (:1)
     """, [fio_users, date_current_oracle])
    result_equip_smartspeaker = cur_ora_rtk.fetchone()[0]
    if result_equip_smartspeaker is None:
        result_equip_smartspeaker = 0
    result['equip_smartspeaker'] = result_equip_smartspeaker

    """ Количество услуг МВНО """
    cur_ora_rtk.execute("""
    select sum(CNT_MVNO) from AGP_V_UNITED_REPORT_RES_LN where AGENT = (:1) and PERIOD = (:1)
     """, [fio_users, date_current_oracle])
    result_mvno = cur_ora_rtk.fetchone()[0]
    if result_mvno is None:
        result_mvno = 0
    result['mvno'] = result_mvno

    """ Количество услуг ОТА """
    cur_ora_rtk.execute("""
    select sum(CNT_OTA) from AGP_V_UNITED_REPORT_RES_LN where AGENT = (:1) and PERIOD = (:1)
     """, [fio_users, date_current_oracle])
    result_ota = cur_ora_rtk.fetchone()[0]
    if result_ota is None:
        result_ota = 0
    result['ota'] = result_ota

    """ Количество услуг Винк+ """
    cur_ora_rtk.execute("""
    select sum(CNT_EQUIP_ANDRSTB) from AGP_V_UNITED_REPORT_RES_LN where AGENT = (:1) and PERIOD = (:1)
     """, [fio_users, date_current_oracle])
    result_equip_andrstb = cur_ora_rtk.fetchone()[0]
    if result_equip_andrstb is None:
        result_equip_andrstb = 0
    result['equip_andrstb'] = result_equip_andrstb

    """ Количество услуг ВН """
    cur_ora_rtk.execute("""
    select sum(CNT_EQUIP_VIDEO) from AGP_V_UNITED_REPORT_RES_LN where AGENT = (:1) and PERIOD = (:1)
     """, [fio_users, date_current_oracle])
    result_equip_video = cur_ora_rtk.fetchone()[0]
    if result_equip_video is None:
        result_equip_video = 0
    result['equip_video'] = result_equip_video

    return result


""" ********************************* ОДИНОЧНЫЕ ПОДКЛЮЧЕННЫЕ УСЛУГИ ПЛАН ************************************** """


@pytest.fixture()
def connected_services_types_graph_plan(id_users, fio_users):

    result = dict()
    cur = conn.cursor()

    ''' Количество услуг ШПД '''
    indicator_id_shpd = 2899
    cur.execute("""
    select sum(value) from plan_objects_indicators
    join objects_indicators on objects_indicators.id = plan_objects_indicators.object_indicator_id
    join users on users.id = objects_indicators.object_id
    where indicator_id = %s and users.id = %s and period_begin between %s and %s
    """, (indicator_id_shpd, id_users, first_period, end_period,))

    result_shpd = cur.fetchone()[0]
    if result_shpd is None:
        result_shpd = 0
    result['shpd'] = result_shpd

    ''' Количество услуг Гарантия '''
    indicator_id_dgo = 2904
    cur.execute("""
        select sum(value) from plan_objects_indicators
        join objects_indicators on objects_indicators.id = plan_objects_indicators.object_indicator_id
        join users on users.id = objects_indicators.object_id
        where indicator_id = %s and users.id = %s and period_begin between %s and %s
        """, (indicator_id_dgo, id_users, first_period, end_period,))

    result_dgo = cur.fetchone()[0]
    if result_dgo is None:
        result_dgo = 0
    result['dgo'] = result_dgo

    ''' Количество услуг ИТВ '''
    indicator_id_iptv = 2900
    cur.execute("""
        select sum(value) from plan_objects_indicators
        join objects_indicators on objects_indicators.id = plan_objects_indicators.object_indicator_id
        join users on users.id = objects_indicators.object_id
        where indicator_id = %s and users.id = %s and period_begin between %s and %s
        """, (indicator_id_iptv, id_users, first_period, end_period,))

    result_iptv = cur.fetchone()[0]
    if result_iptv is None:
        result_iptv = 0
    result['iptv'] = result_iptv

    ''' Количество услуг Маруся '''
    indicator_id_equip_smartspeaker = 2905
    cur.execute("""
        select sum(value) from plan_objects_indicators
        join objects_indicators on objects_indicators.id = plan_objects_indicators.object_indicator_id
        join users on users.id = objects_indicators.object_id
        where indicator_id = %s and users.id = %s and period_begin between %s and %s
        """, (indicator_id_equip_smartspeaker, id_users, first_period, end_period,))

    result_equip_smartspeaker = cur.fetchone()[0]
    if result_equip_smartspeaker is None:
        result_equip_smartspeaker = 0
    result['equip_smartspeaker'] = result_equip_smartspeaker

    ''' Количество услуг МВНО '''
    indicator_id_mvno = 2902
    cur.execute("""
        select sum(value) from plan_objects_indicators
        join objects_indicators on objects_indicators.id = plan_objects_indicators.object_indicator_id
        join users on users.id = objects_indicators.object_id
        where indicator_id = %s and users.id = %s and period_begin between %s and %s
        """, (indicator_id_mvno, id_users, first_period, end_period,))

    result_mvno = cur.fetchone()[0]
    if result_mvno is None:
        result_mvno = 0
    result['mvno'] = result_mvno

    ''' Количество услуг ОТА '''
    indicator_id_ota = 2901
    cur.execute("""
        select sum(value) from plan_objects_indicators
        join objects_indicators on objects_indicators.id = plan_objects_indicators.object_indicator_id
        join users on users.id = objects_indicators.object_id
        where indicator_id = %s and users.id = %s and period_begin between %s and %s
        """, (indicator_id_ota, id_users, first_period, end_period,))

    result_ota = cur.fetchone()[0]
    if result_ota is None:
        result_ota = 0
    result['ota'] = result_ota

    ''' Количество услуг Винк+ '''
    indicator_id_equip_andrstb = 2906
    cur.execute("""
        select sum(value) from plan_objects_indicators
        join objects_indicators on objects_indicators.id = plan_objects_indicators.object_indicator_id
        join users on users.id = objects_indicators.object_id
        where indicator_id = %s and users.id = %s and period_begin between %s and %s
        """, (indicator_id_equip_andrstb, id_users, first_period, end_period,))

    result_equip_andrstb = cur.fetchone()[0]
    if result_equip_andrstb is None:
        result_equip_andrstb = 0
    result['equip_andrstb'] = result_equip_andrstb

    ''' Количество услуг ВН '''
    indicator_id_equip_video = 2903
    cur.execute("""
        select sum(value) from plan_objects_indicators
        join objects_indicators on objects_indicators.id = plan_objects_indicators.object_indicator_id
        join users on users.id = objects_indicators.object_id
        where indicator_id = %s and users.id = %s and period_begin between %s and %s
        """, (indicator_id_equip_video, id_users, first_period, end_period,))

    result_equip_video = cur.fetchone()[0]
    if result_equip_video is None:
        result_equip_video = 0
    result['equip_video'] = result_equip_video

    return result


""" ********************************* ЗАНЯТОСТЬ - ЗАНЯТОСТЬ ************************************** """


@pytest.fixture()
def employment_detail_work():
    cur_n_rtk = connection.cursor()

    cur_n_rtk.execute("""
    
    select ((select sum(duration)/3600 from mv_user_status_full_daily
    where login = %s
    and date_work between %s and %s
    and (normal = 'normal' or ringing = 'ringing' or speaking = 'speaking'))*100)
    /
    (select sum(duration)/3600 from mv_user_status_full_daily
    where login = %s
    and date_work between %s and %s
    and (normal = 'normal' or ringing = 'ringing' or speaking = 'speaking' or wrapup = 'wrapup'))

    """, (login_non_volga, first_period, end_period, login_non_volga, first_period, end_period,))

    result_sql = cur_n_rtk.fetchone()[0]

    if result_sql is None:
        result_sql = 0

    return result_sql


""" ********************************* ЗАНЯТОСТЬ - ПОСТОБРАБОТКА ************************************** """


@pytest.fixture()
def employment_detail_idle():
    cur_n_rtk = connection.cursor()

    cur_n_rtk.execute("""

    select ((select sum(duration)/3600 from mv_user_status_full_daily
    where login = %s
    and date_work between %s and %s
    and wrapup = 'wrapup' and normal is null and ringing is null and speaking is null)*100)
    /
    (select sum(duration)/3600 from mv_user_status_full_daily
    where login = %s
    and date_work between %s and %s
    and (normal = 'normal' or ringing = 'ringing' or speaking = 'speaking' or wrapup = 'wrapup'))

    """, (login_non_volga, first_period, end_period, login_non_volga, first_period, end_period,))

    result_sql = cur_n_rtk.fetchone()[0]

    if result_sql is None:
        result_sql = 0

    return result_sql


""" ********************************* ВРЕМЯ ОФОРМЛЕНИЯ ЗАЯВКИ ************************************** """


@pytest.fixture()
def talk_time_to_connected_services():

    """
    Вычисление времени, затраченного на оформление одной заявки:
    talk_time в таблице подключенных услуг для строк с результатом звонка "Согласие клиента"
    """

    cur_n_rtk = connection.cursor()
    cur_n_rtk.execute("""
    
    select (select sum(talk_time) from mv_user_calls_result_daily
    where login = %s
    and date_work between %s and %s
    and "result" in ('Согласие клиента'))

    """, (login_non_volga, first_period, end_period,))

    result_sql = cur_n_rtk.fetchone()[0]

    if result_sql is None:
        result_sql = 0

    return result_sql
