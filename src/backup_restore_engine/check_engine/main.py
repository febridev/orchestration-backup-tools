import json
import os
from datetime import date, datetime, timedelta
import time
from sqlalchemy import column, create_engine, text,insert,Table,MetaData,select,join,func,update
from sqlalchemy.exc import SQLAlchemyError
import subprocess
import shlex
from croniter import CroniterBadTypeRangeError, croniter
import backup_restore_engine.instance_clone as ci
import backup_restore_engine.patch_instance as pi
import backup_restore_engine.grant_instance as gi
from ..destroy import main as de_main
from ..backup_engine import main as backup_main, exec_sql_export as backup_exec, submit_final_log
from ..notification import create_message as notif
from ..auth import gcloud_login
from dotenv import load_dotenv
import asyncio



#load enviroment variable
load_dotenv()

gcloud_login(os.environ.get("AUTH_EMAIL"),os.environ.get("SERVICE_ACCOUNT"))



def get_db_connection():
    userdb = os.environ.get("USER_DB_DISCOVERY")
    passdb = os.environ.get("PASS_DB_DISCOVERY")
    hostdb = os.environ.get("HOST_DB_DISCOVERY")
    portdb = os.environ.get("PORT_DB_DISCOVERY")
    dbname = os.environ.get("DB_NAME")
    database_url = f"mysql+mysqlconnector://{userdb}:{passdb}@{hostdb}:{portdb}/{dbname}"
    engine = create_engine(database_url, pool_pre_ping=True)
    return engine


def get_running_backup(job_header_id,job_type,job_status,instance_sandbox,dbname=""):
    """
        get running backup Process
        base on job_header_id 
    """
    engine = get_db_connection()
    metadata = MetaData()
    tthjob = Table('tthjob',metadata,autoload_with=engine)
    ttdjob = Table('ttdjob',metadata,autoload_with=engine)
    try:
        backup_running = []
        with engine.connect() as dbconnection:
            if dbname == "":
                statement = select(func.count('*').label("count"), ttdjob.c.seq_id.label("job_detail_id")).\
                select_from(tthjob).join(ttdjob, tthjob.c.seq_id == ttdjob.c.job_header_id).\
                where(func.date(tthjob.c.created_date) == func.date(func.now())).\
                where(tthjob.c.job_type == f"{job_type}").\
                where(ttdjob.c.job_status == f"{job_status}").\
                where(func.substring_index(ttdjob.c.job_header_object,'|',-1) == f"{instance_sandbox}").\
                group_by(ttdjob.c.seq_id)
            else:
                statement = select(func.count('*').label("count"), ttdjob.c.seq_id.label("job_detail_id")).\
                select_from(tthjob).join(ttdjob, tthjob.c.seq_id == ttdjob.c.job_header_id).\
                where(func.date(tthjob.c.created_date) == func.date(func.now())).\
                where(tthjob.c.job_type == f"{job_type}").\
                where(ttdjob.c.job_status == f"{job_status}").\
                where(func.substring_index(ttdjob.c.job_header_object,'|',-1) == f"{instance_sandbox}").\
                where(func.substring_index(ttdjob.c.job_detail_object,'|',1) == f"{dbname}").\
                group_by(ttdjob.c.seq_id)
            # where(tthjob.c.seq_id == f"{job_header_id}").\


            for row in dbconnection.execute(statement):
                backup_running.append(dict(row._mapping))
        return backup_running
    except SQLAlchemyError as e:
        print(f"database Error occured: {e}")
    except Exception as e:
        print(f"An error occured: {e}")




    

def get_list_job_backup(job_header_id,job_detail_id):
    """
        get job detail for backup database
    """
    engine = get_db_connection()
    metadata = MetaData()
    tthjob = Table('tthjob',metadata,autoload_with=engine)
    ttdjob = Table('ttdjob',metadata,autoload_with=engine)

    try:
        list_backup_job = []
        with engine.connect() as dbconnection:
            statement = select(tthjob,ttdjob).\
            select_from(tthjob).join(ttdjob, tthjob.c.seq_id == ttdjob.c.job_header_id).\
            where(func.date(tthjob.c.created_date) == func.date(func.now())).\
            where(tthjob.c.seq_id == f"{job_header_id}").\
            where(tthjob.c.job_type == 2).\
            where(ttdjob.c.seq_id == f"{job_detail_id}").\
            where(ttdjob.c.job_status.in_(["0","1"])).\
            limit(1)
            # where(ttdjob.c.job_status == 0).\
            # where(ttdjob.c.seq_id == f"{job_detail_id}").\

            for row in dbconnection.execute(statement):
                list_backup_job.append(dict(row._mapping))
        return list_backup_job
    except SQLAlchemyError as e:
        print(f"database Error occured: {e}")
    except Exception as e:
        print(f"An error occured: {e}")




        

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
        # print(statement)
        with conn as dbconnection:
            for row in dbconnection.execute(statement):
                job_list.append(dict(row._mapping))

    except Exception as e:
        print(f"error[get_list_job] - {e}")
        exit()


    return job_list


def update_header_job(job_header_id,job_status):
    """
        this function is for update header job status
    """
    base_command = os.environ.get("GCLOUD_COMMAND_OPERATION")
    engine =  get_db_connection()
    metadata = MetaData()
    tthjob = Table('tthjob',metadata,autoload_with=engine)

    try:
        with engine.connect() as dbconnection:
            stmt = (update(tthjob).
                    where(tthjob.c.seq_id == job_header_id).
                    values(job_status = job_status)
                )
            dbconnection.execute(stmt)
            dbconnection.commit()

        msg = f"Success Updated Job Header Id: {job_header_id}"
        return msg
    except SQLAlchemyError as e:
        print(f"Database error {e}")  
    except Exception as e:
        print(f"An error accoured {e}")

def update_detail_job(job_detail_id,job_status,operation_id,project_name,instance_name):
    """
        update detail job 
        if job list already submitted 
    """
    base_command = os.environ.get("GCLOUD_COMMAND_OPERATION")
    engine =  get_db_connection()
    metadata = MetaData()
    ttdjob = Table('ttdjob',metadata,autoload_with=engine)

    try:
        filter = f"--filter='name:{operation_id}'"
        cmd_operation = (f"{base_command}"
                        f" --instance={instance_name} --project={project_name}"
                        f" {filter} --format=json" 
                    )
        # print(cmd_operation)
        
        # # execute command gcloud sql operation list
        cmd_operation = shlex.split(cmd_operation)
        cmd_operation_output = subprocess.run(cmd_operation,capture_output=True)
        cmd_operation_output = json.loads(cmd_operation_output.stdout)
        statusjob = cmd_operation_output[0]['status']
        print(statusjob)
        if statusjob == 'RUNNING':
            statusjob = 1
        elif statusjob == 'DONE':
            statusjob = 2
        else:
            statusjob = 9
        # print(statusjob)

        with engine.connect() as dbconnection:
            stmt = (update(ttdjob).
                    where(ttdjob.c.seq_id == job_detail_id).
                    values(job_status = statusjob).
                    values(job_detail_object = func.regexp_replace(ttdjob.c.job_detail_object, r'\|-', f"|{operation_id}", 1))
                )
            dbconnection.execute(stmt)
            dbconnection.commit()
        
        
        msg = f"Success Updated Job Detail Id: {job_detail_id}"
        return msg
    except SQLAlchemyError as e:
        print(f"Database error {e}")  
    except Exception as e:
        print(f"An error accoured {e}")


    



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
    sent_notifications = set()

    database_url = f"mysql+mysqlconnector://{userdb}:{passdb}@{hostdb}:{portdb}/{dbname}"

    engine = create_engine(database_url)
    conn = engine.connect()
    metadata = MetaData()

    tthjob = Table('tthjob',metadata,autoload_with=engine)
    ttdjob = Table('ttdjob',metadata,autoload_with=engine)

    stmt = ""
    header_job_id = ""

    # print(joblist)

    # set base command gcloud sql operation list
    base_command = os.environ.get("GCLOUD_COMMAND_OPERATION")

    # get check job
    for dtjob in joblist:
        if dtjob['job_type'] in [1,6,7,8]:
            job_type = dtjob['job_type']
            project_name = f"--project={dtjob['job_header_object'].split("|")[1]}"
            detail_job_id = dtjob['seq_id_1']
            header_job_id = dtjob['job_header_id']
            operation_name = f"{dtjob['job_detail_object'].split('|')[0]}"
            operation_id = f"{dtjob['job_detail_object'].split('|')[1]}"
            sandbox_instance = dtjob['job_detail_object'].split('|')[2]
            instance_name = f"--instance={dtjob['job_detail_object'].split('|')[2]}"
            filter = f"--filter='name:{operation_id}'"

            cmd_operation = (f"{base_command}"
                            f" {instance_name} {project_name}"
                            f" {filter} --format=json" 
                        )

            # execute command gcloud sql operation list
            cmd_operation = shlex.split(cmd_operation)
            cmd_operation_output = subprocess.run(cmd_operation,capture_output=True)
            cmd_operation_output = json.loads(cmd_operation_output.stdout)
            # print(cmd_operation_output)

            statusjob = cmd_operation_output[0]['status']

            print(f"Task - {cmd_operation_output[0]['operationType']} Instance {cmd_operation_output[0]['targetId']} Status {statusjob}")

            if statusjob == 'DONE':
                # update job detail ttdjob status done 
                jobstatus=2
                stmt = (
                    update(ttdjob).
                        where(ttdjob.c.seq_id == detail_job_id).
                    values(job_status = f"{jobstatus}")
                )
                # print(stmt)
                conn3 = engine.connect()
                conn3.execute(stmt)
                conn3.commit()

                notif_params = (dtjob['job_header_object'].split("|")[1], sandbox_instance)
                print(f"Notif Params: {notif_params}")
                # check all detail job if all done update header job
                stmt = (select(func.count('*')).select_from(ttdjob).\
                    where(ttdjob.c.job_header_id == header_job_id).\
                    where(ttdjob.c.job_status.in_(["0","1"])))
                total_data = []
                with conn3 as dbconnection:
                        for row in dbconnection.execute(stmt):
                            total_data.append(dict(row._mapping))
                total_data = total_data[0]["count_1"]
                if total_data == 0:
                    stmt =(
                        update(tthjob).\
                        where(tthjob.c.seq_id == header_job_id).\
                        values(job_status = jobstatus)
                    )
                    conn = engine.connect()
                    conn.execute(stmt)
                    conn.commit()

                    if job_type == 1:
                        # send notification
                        if notif_params not in sent_notifications:
                            notif("success","Clone Instance",dtjob['job_header_object'].split("|")[1],'-',sandbox_instance)
                            sent_notifications.add(notif_params)
                        
                        #execute patch_instance
                        print("Execute Patch Instance and Grant Instance To GCS Standard")
                        gi.main(dtjob['job_header_object'].split("|")[1],sandbox_instance)
                        pi.main(dtjob['job_header_object'].split("|")[1],sandbox_instance)
                    elif job_type == 6:
                        # send notification
                        notif("success","Grant & Patch Sanbox Backup Instances",dtjob['job_header_object'].split("|")[1],'-',sandbox_instance)
                        #execute grant_instance
                        print("Execute backup")
                        backup_main(dtjob['job_header_object'].split("|")[1], sandbox_instance)

                else:
                    print("Other Process Still Running")



            if statusjob == 'RUNNING':
                # update job detail ttdjob status done 
                jobstatus=1
                stmt = (
                    update(ttdjob).
                        where(ttdjob.c.seq_id == detail_job_id).
                    values(job_status = f"{jobstatus}")
                )
                conn2 = engine.connect()
                conn2.execute(stmt)
                conn2.commit()

        # execute backup process base on job list
        if dtjob['job_type'] == 2:
            print(dtjob['seq_id'])
            print(dtjob['seq_id_1'])
            job_type = dtjob['job_type']
            project_name = f"{dtjob['job_header_object'].split("|")[1]}"
            detail_job_id = dtjob['seq_id_1']
            header_job_id = dtjob['job_header_id']
            operation_name = f"{dtjob['job_detail_object'].split('|')[0]}"
            operation_id = f"{dtjob['job_detail_object'].split('|')[1]}"
            sandbox_instance = dtjob['job_detail_object'].split('|')[2]
            instance_name = f"{dtjob['job_header_object'].split('|')[2]}"



            backup_job_list = get_list_job_backup(dtjob['seq_id'],dtjob['seq_id_1'])
            for row in backup_job_list:
                database_name = f"{row['job_detail_object'].split('|')[0]}"
                table_name = f"{row['job_detail_object'].split('|')[2]}"
                sandbox_instances = f"{row['job_header_object'].split('|')[2]}"
                if row['job_status_1'] == 0:
                    # check existing process
                    process_found = get_running_backup(row['seq_id'],job_type,1,sandbox_instances)
                    print(process_found)
                    if len(process_found) == 0 or process_found[0]['count'] == 0:
                        print(f"Executing Backup job detail id: {row['seq_id_1']}")
                        if table_name.strip() == "-":
                            table_name = ""
                        else:
                            table_name 
                        print(f"table value {table_name}")
                        export = backup_exec(project_name,instance_name,database_name,table_name)
                        operation_id = export['name']
                        #setup command
                        update_header_job(row['seq_id'],1)
                        update_detail_job(row['seq_id_1'],1,operation_id,project_name,instance_name)
                        continue
                    else:
                        print(f"Job Detail ID {process_found[0]['job_detail_id']} Still Running")
                elif row['job_status_1'] == 1:
                    print(row['job_status_1'])
                    print(f"job_status {row['seq_id_1']}")
                    update_detail_job(row['seq_id_1'],1,operation_id,project_name,instance_name)
                # elif row['job_status_1'] == 2:
                    process_found = get_running_backup(row['seq_id'],job_type,0,sandbox_instances,database_name)
                    process_run_found = get_running_backup(row['seq_id'],job_type,1,sandbox_instances,database_name)
                    print(len(process_found))
                    print(process_found)
                    print(process_run_found)
                    if len(process_found) == 0 or process_found[0]['count'] == 0:
                        if len(process_run_found) == 0 or process_run_found[0]['count'] == 0:
                            print("Job detail with status running does not exists")
                            print("update header job job status DONE")
                            #update header job job_status = 2
                            update_header_job(row['seq_id'],2)

                            #submit final backup log
                            print("Submit Final Backup Log")
                            submit_final_log(project_name,instance_name,database_name,'SUCCESS')

                            # check untuk multiple database
                            all_process_found = get_running_backup(row['seq_id'],job_type,0,sandbox_instances)
                            if len(all_process_found) == 0 or all_process_found[0]['count'] == 0:
                                #execute drop instances sandbox backup
                                print(f"Destroy Clone Instance {instance_name}")
                                de_main(project_name,instance_name)
                        else:
                            print(f"Job Detail ID {process_run_found[0]['job_detail_id']} Still Running")
                    else:
                        print(f"Job Detail ID {process_found[0]['job_detail_id']} Still Running")
                else:
                    print(f"Job Detail ID: {row['seq_id_1']} Still Running xx")
                    pass
                break
            









def main():
    """
        get instance ready to backup base on tbackup_config
    """
    engine = get_db_connection()
    metadata = MetaData()
    tbackup_config = Table('tbackup_config',metadata,autoload_with=engine)

    while True:
        try:
            list_instances = []
            with engine.connect() as dbconnection:
                statement = select(tbackup_config).select_from(tbackup_config).where(tbackup_config.c.backup_status == 'enabled')
                for row in dbconnection.execute(statement):
                    list_instances.append(dict(row._mapping))

            now = datetime.now()
            cronformat = f"{now.minute} {now.hour} * * *"
            for linstances in list_instances:
                cron_now = croniter(linstances['param_cron'],now)
                next_time = cron_now.get_next(datetime)

                # print(f"{linstances['param_cron']} - {cronformat}")

                if linstances['param_cron'] == cronformat:
                    print("Cron Existing")
                    print("Execute Instances Clone")
                    ci.main(linstances['project_name'],linstances['instance_prod'])
                else:
                    print("No Others Database Servers Found To Backup")
                
        except SQLAlchemyError as e:
            print (f"database Error occured: {e}")
        except Exception as e:
            print(f"An error occured: {e}")

        # execute Check Job
        get_operations_log(get_list_job())
        time.sleep(60)




if __name__ == "__main__":
    main()
