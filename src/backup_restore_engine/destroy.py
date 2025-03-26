import json
import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import column, create_engine, text,insert,Table,MetaData,select,Join
from sqlalchemy.exc import SQLAlchemyError
import subprocess
import shlex
import backup_restore_engine.vault as va
from dotenv import load_dotenv
import backup_restore_engine.auth as ath
from .notification import create_message as notif


#load_env()
load_dotenv()

# call login gcloud
ath.gcloud_login(os.environ.get("AUTH_EMAIL"),os.environ.get("SERVICE_ACCOUNT"))

def get_db_connection():
    userdb = os.environ.get("USER_DB_DISCOVERY")
    passdb = os.environ.get("PASS_DB_DISCOVERY")
    hostdb = os.environ.get("HOST_DB_DISCOVERY")
    portdb = os.environ.get("PORT_DB_DISCOVERY")
    dbname = os.environ.get("DB_NAME")
    database_url = f"mysql+mysqlconnector://{userdb}:{passdb}@{hostdb}:{portdb}/{dbname}"
    engine = create_engine(database_url, pool_pre_ping=True)
    return engine


def get_clone_instance(project_name, instance_name):
    """
        this function get all clone instance with prefix 'sandbox-backup'
        base on projectname and return as json 
    """
    list_instances = []
    engine = get_db_connection()
    metadata = MetaData()

    # define table
    tbackup_config = Table('tbackup_config',metadata,autoload_with=engine)

    try:
        with engine.connect() as dbconnection:
            statement = select(tbackup_config.c.project_name, tbackup_config.c.instance_sandbox).\
            select_from(tbackup_config).\
            where(tbackup_config.c.backup_status == 'enabled').\
            where(tbackup_config.c.project_name == f"{project_name}").\
            where(tbackup_config.c.instance_sandbox == f"{instance_name}")
            for row in dbconnection.execute(statement):
                list_instances.append(dict(row._mapping))
        return list_instances
    except SQLAlchemyError as e:
        print(f"database Error occured: {e}")
    except Exception as e:
        print(f"destroy-instance - {e}")
        exit()

def main(project_name,instance_name):
    """
    execute to destroy instance with subfix 'sandbox-backup'
    """
    base_command = os.environ.get("GCLOUD_COMMAND_DELETE")
    filters = f"--async"
    filter_project = f"--project={project_name}"
    delete_command = (
        f"{base_command} {instance_name} {filters} {filter_project} --quiet"
    )
    engine = get_db_connection()
    metadata = MetaData()

    # define table
    tthjob = Table('tthjob',metadata,autoload_with=engine)
    ttdjob = Table('ttdjob',metadata,autoload_with=engine)
    try:
        with engine.connect() as dbconnection:
            # execute insert tthjob
            # statement = text("""INSERT INTO tthjob (job_type,job_status,created_by,updated_by) VALUES (8,0,'friday','friday');""")
            statement = insert(tthjob).values(job_type=8,job_status=2,created_by='friday',updated_by='friday')
            result = dbconnection.execute(statement)
            jobheader_id = result.inserted_primary_key[0]

        # insert job detail 
        # get instances
            lsinstances = get_clone_instance(project_name,instance_name)

            for rinstance in lsinstances:
                delete_command = (
                    f"{base_command} {rinstance['instance_sandbox']} {filters} {filter_project} --quiet"
                )
                cmd_delete = shlex.split(delete_command)
                cmd_delete_output = subprocess.run(cmd_delete,capture_output=True)
                # cmd_delete_output = json.loads(cmd_delete_output.stdout)

                #insert detail
                status = 2
                alias = os.environ.get("ALIAS")
                task = f"DELETE INSTANCE|{project_name}"
                operation_name = "DROP INSTANCE"
                operation_type = "DROP INSTANCE"
                instance_sandbox = rinstance['instance_sandbox']
                detail = f"{operation_type}|{operation_name}|{instance_sandbox}" 
                    
                sql2 = insert(ttdjob).\
                    values(job_header_id=jobheader_id,\
                        job_header_object=task,\
                        job_detail_object=detail,\
                        job_status=status,\
                        created_by = alias, updated_by = alias)
                # print(sql2)
                dbconnection.execute(sql2)
            dbconnection.commit()
            dbconnection.close()
            notif("success","Destroy Sanbox Backup Instance",project_name,"-",instance_sandbox)
    except SQLAlchemyError as e:
        print(f"database Error occured: {e}")
        notif("failed","Destroy Sanbox Backup Instance",project_name,"-",instance_sandbox)
    except subprocess.CalledProcessError as e:
        print(f"Error executing gcloud command: {e}")
        notif("failed","Destroy Sanbox Backup Instance",project_name,"-",instance_sandbox)
        raise
    except json.JSONDecodeError as e:
        print(f"Error parsing gcloud command output: {e}")
        notif("failed","Destroy Sanbox Backup Instance",project_name,"-",instance_sandbox)
        raise
    except Exception as e:
        print(f"destroy_instance - {e}")
        notif("failed","Destroy Sanbox Backup Instance",project_name,"-",instance_sandbox)
        # exit()




