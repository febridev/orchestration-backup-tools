import json
import os
from datetime import datetime, timedelta
import time
from zoneinfo import ZoneInfo
from sqlalchemy import column, create_engine, text,insert,Table,MetaData,select,join,func,update
import subprocess
import shlex
import backup_restore_engine.vault as va
from dotenv import load_dotenv



#load enviroment variable
load_dotenv()

def get_list_job():
    """
        this function get data from detail job ttdjob and check job status pending
    """
    job_list = []
    userdb = os.environ.get("USER_DB_DISCOVERY")
    passdb = os.environ.get("PASS_DB_DISCOVERY")
    hostdb = os.environ.get("HOST_DB_DISCOVERY")
    portdb = os.environ.get("PORT_DB_DISCOVERY")
    dbname = os.environ.get("DB_NAME")

    database_url = f"mysql+mysqlconnector://{userdb}:{passdb}@{hostdb}:{portdb}/{dbname}"
    try:
        engine = create_engine(database_url)
        conn = engine.connect()
        metadata = MetaData()
        tthjob = Table('tthjob',metadata,autoload_with=engine)
        ttdjob = Table('ttdjob',metadata,autoload_with=engine)
        statement = select(tthjob,ttdjob).\
                    select_from(tthjob).join(ttdjob, tthjob.c.seq_id == ttdjob.c.job_header_id).\
                    where(func.date(tthjob.c.created_date) == func.date(func.now())).\
                    where(ttdjob.c.job_status.in_(["0","1"]))
                    # where(tthjob.c.job_type == job_type).\
        with conn as dbconnection:
            for row in dbconnection.execute(statement):
                job_list.append(dict(row._mapping))

    except Exception as e:
        print(f"error[get_list_job] - {e}")
        exit()


    return job_list
    



def get_operations_log(joblist):
    """
        this function is get operation log list from cloudsql
        base on joblist from database where job header is clone instance
        and job status pending 0
    """

    userdb = os.environ.get("USER_DB_DISCOVERY")
    passdb = os.environ.get("PASS_DB_DISCOVERY")
    hostdb = os.environ.get("HOST_DB_DISCOVERY")
    portdb = os.environ.get("PORT_DB_DISCOVERY")
    dbname = os.environ.get("DB_NAME")

    database_url = f"mysql+mysqlconnector://{userdb}:{passdb}@{hostdb}:{portdb}/{dbname}"

    engine = create_engine(database_url)
    conn = engine.connect()
    metadata = MetaData()

    tthjob = Table('tthjob',metadata,autoload_with=engine)
    ttdjob = Table('ttdjob',metadata,autoload_with=engine)

    stmt = ""
    header_job_id = 6 


    # set base command gcloud sql operation list
    base_command = os.environ.get("GCLOUD_COMMAND_OPERATION")
    stmt = (select(func.count('*')).select_from(ttdjob).\
                where(ttdjob.c.job_header_id == header_job_id).\
                where(ttdjob.c.job_status.in_(["0","1"])))
    total_data = []
    with conn as dbconnection:
            for row in dbconnection.execute(stmt):
                total_data.append(dict(row._mapping))
    

    total_data = total_data[0]["count_1"]
    if total_data == 0:
        stmt2 =(
            update(tthjob).\
            where(tthjob.c.seq_id == header_job_id).\
            values(job_status = 2)
        )
        print(stmt2)
        conn = engine.connect()
        conn.execute(stmt2)
        conn.commit()






if __name__ == "__main__": 
    get_operations_log(get_list_job())
