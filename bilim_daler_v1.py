#!/usr/bin/python3.6

# LIBS
import pyodbc
import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime, date, timedelta
import time
from progress.bar import IncrementalBar
import logging
import sys

LOG_FILE = '/data/infa/1056/server/infa_shared/Custom_scripts/logs/bilim_daler_v1_final_score.log'

file_handler = logging.FileHandler(filename=LOG_FILE, mode='a')
# , encoding='utf-8'

logging.basicConfig(level=logging.ERROR ,  # INFO, ERROR , DEBUG
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[file_handler]
                    )

# to handle unexpected errors
def handle_exception(exc_type, exc_value, exc_traceback):
    if exc_type is not KeyboardInterrupt:
        logging.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
    sys.exit(1)

sys.excepthook = handle_exception


# ---------------------------------------------------------------------------

drivers = [item for item in pyodbc.drivers()]
# driver = '/opt/infa/1052/ODBC7.1/lib/DWpsql27r.so'#drivers[0] #postgresql - greenplum
driver = drivers[0]
server = '172.17.138.48'
database = 'cashbdb'
uid = 'pgadmin'
pwd = 'pDLKCA6DWg'

conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}'

headers = {
    "X-SECRET-TOKEN": "CnLW^AZFubtNmD+Jr;Q5G$(1@7@ojvx'7>t~`AVe'Q.kd$(c+.zoZJp5`!))B<.",
}


for i in range(100):

    logging.info("Start")

    sql_get_max_uuid = '''select * from (select mark_uuid 
                                        from loyalty.bilim_marks_forpay
                                        order by mark_uuid desc) 
                        limit 1'''
    logging.info(f"Connection to DB.  Task: execute_sql {sql_get_max_uuid}")

    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_get_max_uuid)
            result = cursor.fetchone()


    if len(result) > 0 and result is not None:
        str1 = '?uuid=' + str(result[0]).lower()

        logging.info(f"Connection to DB. Get max UUID {str1}")
        logging.info(f"Connection to API - Start")

        try:
            response = requests.get('https://api.bilimclass.kz/api/v1/external/marks' + str1, headers=headers)
            logging.debug(f'Connection to API - Get status code {str(response.status_code)}')
            if response.status_code !=200:
                logging.error(f'Connection to API - Get status code {str(response.status_code)}')
                time.sleep(10)
                continue
            resp_data = response.json()['data']
        except Exception as err:
            logging.error(f'Connection to API -  {response.json()}  ERROR : {err}')
            break

    else:
        logging.error(f"Connection to API.  Task: execute_sql - Warning , no data {sql_get_max_uuid}")
        str1 = ''
        resp_data = []
        break  # just in case

    logging.info(f"Connection to API - End")
    logging.debug(f'Connection to API - Response data len = {str(len(resp_data))}')

    if len(resp_data) < 500:
        logging.error(f'Connection to API (не ошибка)- Response data len = {str(len(resp_data))}')
        break

    bar = IncrementalBar('Countdown', max=len(resp_data))
    data = []

    logging.info(f"Mark processing - Start")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for i in range(len(resp_data)):
        if (  # "mark" in resp_data[i]["newValue"]
                # resp_data[i]["dataTypeChanged"] in ('final_score')
                resp_data[i]["dataTypeChanged"] in (
        'student_mark_formatted_score', 'student_mark_sor', 'student_mark_soch', 'attestation_quarter', 'final_score')
                and resp_data[i]["entityType"] in ('attestation', 'student_mark', 'final_score')
                and resp_data[i]["actionType"] in ('created', 'updated', 'deleted')):
            new_value = 0
            old_value = 1

            if resp_data[i]["dataTypeChanged"] in ('final_score') :
                if 'year_score' in resp_data[i]["newValue"] :
                    new_value = (new_value if  resp_data[i]["newValue"]["year_score"] is None
                                                or resp_data[i]["newValue"]["year_score"] in ('unattestated', 'exemption', 'notstudy', 'not_certified')
                                    else resp_data[i]["newValue"]['year_score'])
                elif resp_data[i]["oldValue"] is not None and 'year_score' in resp_data[i]["oldValue"] :
                    old_value = (old_value if  resp_data[i]["oldValue"]["year_score"] is None
                                                   or resp_data[i]["oldValue"]["year_score"] in ('unattestated', 'exemption', 'notstudy', 'not_certified')
                                            else resp_data[i]["oldValue"]['year_score'])
            elif resp_data[i]["dataTypeChanged"] in ('student_mark_formatted_score', 'student_mark_sor', 'student_mark_soch','attestation_quarter'):
                if 'mark' in resp_data[i]["newValue"]:
                    new_value = (new_value if resp_data[i]["newValue"]["mark"] is None
                                                  or resp_data[i]["newValue"]["mark"] in (
                                                  'unattestated', 'exemption', 'notstudy', 'not_certified')
                                    else resp_data[i]["newValue"]['mark'])
                elif resp_data[i]["oldValue"] is not None and 'mark' in resp_data[i]["oldValue"]:
                    old_value = (old_value if resp_data[i]["oldValue"]["mark"] is None
                                                  or resp_data[i]["oldValue"]["mark"] in (
                                                  'unattestated', 'exemption', 'notstudy', 'not_certified')
                else resp_data[i]["oldValue"]['mark'])

            new_value=int(new_value)
            old_value=int(new_value)

            mark_max = None

            if resp_data[i]["newValue"] is not None:
                if resp_data[i]["entityType"] in ('student_mark') and 'markMax' in resp_data[i]["newValue"] and resp_data[i]["newValue"]["markMax"] not in ('unattestated', 'exemption', 'notstudy', 'not_certified') :
                    mark_max = resp_data[i]["newValue"]["markMax"]
                elif resp_data[i]["entityType"] in ('final_score') and 'recommended_year_score' in resp_data[i]["newValue"] and resp_data[i]["newValue"]["recommended_year_score"] not in ('unattestated', 'exemption', 'notstudy', 'not_certified') :
                    mark_max = resp_data[i]["newValue"]["recommended_year_score"]

            cashback_sum = 0
            is_return = 0

            # map()
            if resp_data[i]["dataTypeChanged"] in ('student_mark_formatted_score'):
                cashback_sum = 100
            elif resp_data[i]["dataTypeChanged"] in ('student_mark_sor', 'student_mark_soch'):
                cashback_sum = 200
            elif resp_data[i]["dataTypeChanged"] in ('attestation_quarter', 'final_score'):
                cashback_sum = 300
            else:
                cashback_sum = 0


            if resp_data[i]["actionType"] in ['deleted']:
                is_return=1
            if resp_data[i]["actionType"] in ['updated', 'created']:
                if resp_data[i]["dataTypeChanged"] in ('student_mark_formatted_score') and new_value==10 :
                    is_return=0
                elif ( resp_data[i]["dataTypeChanged"] in ('student_mark_sor','student_mark_soch')
                    and mark_max is not None
                    and (new_value/int(mark_max))*100>=85 ) :
                    is_return=0
                    #((marks.new_mark::decimal/marks.markmax::decimal)*100)::decimal(19,2)
                elif  resp_data[i]["dataTypeChanged"] in ('attestation_quarter','final_score') and new_value==5:
                    is_return = 0
                else :
                    is_return = 1
                    if resp_data[i]["actionType"] in ['created']:
                        cashback_sum=0
                        is_return = 0



            data.append((
                datetime.now(),
                resp_data[i]["uuid"],
                resp_data[i]["dateAction"], #datetime.now()
                resp_data[i]["subjectId"],
                resp_data[i]["subjectTitle"],
                old_value,  # 1 , int(resp_data[i]["oldValue"]["mark"] or -1),
                new_value,
                resp_data[i]["dataTypeChanged"],
                resp_data[i]["entityType"],
                resp_data[i]["actionType"],
                resp_data[i]["entityId"],
                resp_data[i]["studentIin"],
                resp_data[i]["createdAt"],
                resp_data[i]["updatedAt"],
                cashback_sum,
                is_return,
                3,
                1,
                mark_max
                # resp_data[i]["cashback_sum"],
                # resp_data[i]["is_return"],
                # resp_data[i]["loyalty_status"],
                # resp_data[i]["is_transaction"]
            ))

            try:
                if len(data) > 0:
                    cursor.executemany(
                        "INSERT INTO loyalty.bilim_marks_forpay (change_date, mark_uuid, dateaction, subjectid, "
                        "subjecttitle, old_mark, new_mark, datatypechanged, entitytype, actiontype, entityid, studentiin, "
                        "createdat, updatedat, cashback_sum, is_return, loyalty_status, is_transaction, markmax) VALUES \
                                                        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?); ",
                        data)
                    cursor.commit()
                    data.clear()
                logging.debug(f'Mark processing - Progress: {i} {resp_data[i]["uuid"]} - processed')
            except pyodbc.IntegrityError as err:
                #logging.error(f'Mark processing - {i} {resp_data[i]["uuid"]}  IntegrityError. ERROR : {err}')
                log_sql = f'''INSERT INTO loyalty.bilim_marks_error_log (change_date, error_message,mark_uuid, dateaction, subjectid, 
                                       subjecttitle, old_mark, new_mark, datatypechanged, entitytype, actiontype, entityid, studentiin, 
                                       createdat, updatedat, cashback_sum, is_return, loyalty_status, is_transaction, markmax) VALUES 
                                                        (?,'IntegrityError',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?); '''
                cursor.executemany(log_sql,
                                   data)
                cursor.commit()
            finally:
                data.clear()  # clean failed rows too

        data.clear() # clean else rows
        bar.next()

    bar.finish()

    logging.info(f"Mark processing - End")

    if cursor:
        cursor.close()
    if conn:
        conn.close()

logging.info(f"Finish")
