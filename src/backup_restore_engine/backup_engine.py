import json
import os
from datetime import date, datetime, timedelta
from sqlalchemy import  create_engine, insert,Table,MetaData,select,Join,text
from sqlalchemy.exc import SQLAlchemyError
from .auth import gcloud_login
import subprocess
import shlex
from dotenv import load_dotenv
from .notification import create_message as notif

#load enviroment variable
load_dotenv()

gcloud_login(os.environ.get("AUTH_EMAIL"),os.environ.get("SERVICE_ACCOUNT"))


def get_backup_type(project_name,instance_name,database_name):
    """
    this function get backup type from tbackup_config
    if 1 will be FULL EACH TABLE if 0 FULL BUNDLE DATABASE
    """
    userdb = os.environ.get("USER_DB_DISCOVERY")
    passdb = os.environ.get("PASS_DB_DISCOVERY")
    hostdb = os.environ.get("HOST_DB_DISCOVERY")
    portdb = os.environ.get("PORT_DB_DISCOVERY")
    dbname = os.environ.get("DB_NAME")
    detail_backup=[]

    statement = text(f""" select b.tablebackup,a.instance_prod, b.gcs_archive_path,a.dbengine,a.parallel 
                     from tbackup_config a 
                     left join tdbackup_config b on a.instance_sandbox = b.instance_sandbox
                     where a.project_name = :project_name
                     and a.instance_sandbox = :instance_sandbox
                     and b.dbname = :dbname """)
    database_url = f"mysql+mysqlconnector://{userdb}:{passdb}@{hostdb}:{portdb}/{dbname}"
    try:
        engine = create_engine(database_url)
        conn = engine.connect()
        with conn as dbconnection:
            result = dbconnection.execute(statement, {"project_name":project_name,"instance_sandbox":instance_name,"dbname":database_name})
            for row in result:
                detail_backup.append(dict(row._mapping))
    except SQLAlchemyError as e:
        print(f"database Error occured: {e}")
    except Exception as e:
        print(f"function get_ip_machine - {e}")
        exit()

    return detail_backup



def get_ip_machine(project_name,instance_name):
    """
        this function get ip and port 
        from sandbox instances
    """
    #setup command
    base_command = "gcloud sql instances describe"
    option_project = f"--project={project_name}"
    filter = f"--format='json(name, ipAddresses, databaseVersion)'"

    fullcommand = (
        f"{base_command} "
        f"{instance_name} "
        f"{filter} "
        f"{option_project} "
    )

    alllist = []


    print(fullcommand)

    #execute command
    try:
        cmd_describe = shlex.split(fullcommand)
        cmd_describe_output = subprocess.run(cmd_describe,capture_output=True)
        cmd_describe_output = json.loads(cmd_describe_output.stdout)


        # GET PORT BASE ON ENGINE DB 
        portdb = cmd_describe_output['databaseVersion'].split('_')[0]

        if portdb == 'MYSQL':
            portdb = 3306
        elif portdb == 'POSTGRES':
            portdb = 5432
        elif portdb == 'SQLSERVER':
            portdb = 1433
        else:
            portdb = portdb

        ipaddress = cmd_describe_output['ipAddresses'][0]['ipAddress']

        alllist = {"name":f"{cmd_describe_output['name']}","ipaddress": f"{ipaddress}", "port":f"{portdb}"}
    except SQLAlchemyError as e:
        print(f"database Error occured: {e}")
    except Exception as e:
        print(f"function get_ip_machine - {e}")
        exit()

    return alllist

def get_all_table(client_dbname,ipdb,portdb,enginedb):
    """
        this function is get all table base on database
        related if option table is True
    """
    # set variable DB
    userdb = os.environ.get("USER_DB_AGENT")
    passdb = os.environ.get("PASS_DB_AGENT")
    hostdb = ipdb
    portdb = portdb
    
    # check engine
    if enginedb == 'MySQL': 
        connector = "mysql+mysqlconnector"
        dbname = client_dbname
        statement = text(f"""
            SELECT TABLE_SCHEMA as table_schema, 
                TABLE_NAME as table_name 
            FROM information_schema.tables 
            WHERE TABLE_SCHEMA = :dbname
        """)
    elif enginedb == 'Postgres':
        connector = "postgresql+psycopg2"
        dbname = client_dbname
        statement = text(f"""
            SELECT table_schema, concat(table_schema,'.',table_name) as table_name
            FROM information_schema.tables 
            WHERE table_catalog = :dbname
            and table_schema not in ('information_schema','pg_catalog')
        """)
    elif enginedb == 'SQLServer':
        connector = ""
    else:
        print("No database driver supported")


    database_url = f"{connector}://{userdb}:{passdb}@{hostdb}:{portdb}/{dbname}"
    print(database_url)

    list_tables = []

    # get all tables
    try:
        engine = create_engine(database_url)
        conn = engine.connect()
        # metadata = MetaData()
        # inf_tables = Table('tables',metadata,autoload_with=engine)

        # statement = select(inf_tables.c.table_schema, inf_tables.c.table_name).\
        #             select_from(inf_tables).\
        #             where(inf_tables.c.table_catalog == f"{dbname}")
        # print(statement)


        with conn as dbconnection:
            result = dbconnection.execute(statement, {"dbname": dbname})
            for row in result: 
                list_tables.append(dict(row._mapping))
    
    except Exception as e:
        print(f"get_all_table - {e} ")
        exit()

    return list_tables

def create_path(project_name,dbname,enginedb,parallel,table_name=""):
    """
        this function is create path backup
        base on database name and base path
    """
    now = datetime.now()
    yearpath = f"{now.year}"
    monthpath = f"{now.month:02d}"
    daypath = f"{now.day:02d}"
    fullday_prefix = f"{yearpath}{monthpath}{daypath}"

    # Common base path
    base_path = f"{dbname}/{yearpath}/{monthpath}/{fullday_prefix}/{fullday_prefix}"

    # Determine the file extension based on parallel and engine
    file_extension = ""
    if enginedb.lower() in ['mysql', 'postgres'] and parallel == 0:
        file_extension = ".sql.gz"
    
    if enginedb.lower() in ['sqlserver']:
        file_extension = ".bak"

    # Handle table_name logic
    if table_name and table_name != "-":
        file_suffix = f"_{table_name}"
    else:
        file_suffix = f"_{dbname}"

    # Combine the base path, file suffix, and extension
    full_path = f"{base_path}{file_suffix}{file_extension}"
        
    return full_path

def get_archive_path(project_name,instance_name,database_name):
    """
        this function is get all instances
        from tbackup_config where 
        crontab is match with currrent datetime
    """
    # set variable DB
    userdb = os.environ.get("USER_DB_DISCOVERY")
    passdb = os.environ.get("PASS_DB_DISCOVERY")
    hostdb = os.environ.get("HOST_DB_DISCOVERY")
    portdb = os.environ.get("PORT_DB_DISCOVERY")
    dbname = os.environ.get("DB_NAME")

    database_url = f"mysql+mysqlconnector://{userdb}:{passdb}@{hostdb}:{portdb}/{dbname}"
    archive_path =[]

    try:
        engine = create_engine(database_url)
        conn = engine.connect()
        metadata = MetaData()
        tbackup_config = Table('tbackup_config',metadata,autoload_with=engine)
        tdbackup_config = Table('tdbackup_config',metadata,autoload_with=engine)
        statement = select(tdbackup_config.c.gcs_archive_path,tbackup_config.c.parallel, tbackup_config.c.dbengine).\
                    select_from(tbackup_config).join(tdbackup_config, tbackup_config.c.instance_sandbox == tdbackup_config.c.instance_sandbox).\
                    where(tbackup_config.c.project_name == f"{project_name}").\
                    where(tbackup_config.c.instance_sandbox == f"{instance_name}").\
                    where(tdbackup_config.c.dbname == f"{database_name}")
        with conn as dbconnection:
            for row in dbconnection.execute(statement):
                archive_path.append(dict(row._mapping))
    except SQLAlchemyError as e:
        print(f"Database Error {e}")
    except Exception as e:
        print(f"get archive path - {e}")
        exit();
    print(archive_path)
    return archive_path



# setup command gcloud sqlexport command
def backup_command(project,instance,database,table,enginedb,base_path="",v_parallel=0,v_async=True,v_quiet=True):
    """
        this function is create command gcloud sql export sql 
        base on parameter and parameter will be put base on check machine
    """

    #setup command base on parameter
    # gcs_path = os.environ.get("GCS_TEMP_PATH")
    if base_path == "":
        gcs_path = f"{os.environ.get("GCS_TEMP_PATH")}/{create_path(project,database,enginedb,v_parallel,table)}"
    else:
        gcs_path = f"{base_path}/{create_path(project,database,enginedb,v_parallel,table)}"

    if enginedb.lower() in ['mysql','postgres']:
        base_command = os.environ.get("GCLOUD_COMMAND_EXPORT")
    else:
        base_command = os.environ.get("GCLOUD_COMMAND_EXPORT_MSSQL")


    # validate required parameters
    if not project:
        raise ValueError("project id parameter is required.")
    if not instance:
        raise ValueError("instance name paramater is required.")
    if not database:
        raise ValueError("database name parameter is required.")

    # check option 
    if enginedb.lower() in ['mysql','postgres']:
        if v_parallel == 0:
            opt_parallel = ""
        else:
            opt_parallel = "--parallel"
        opt_async = "--async" if v_async else ""
        opt_quiet = "--quiet" if v_quiet else ""
        opt_table = f"--table={table}" if table else ""
    else:
        opt_parallel = ""
        opt_async = "--async" if v_async else ""
        opt_quiet = "--quiet" if v_quiet else ""
        opt_table = ""

    
    # concat script gcloud export
    backup_command = (
        f"{base_command} {instance} {gcs_path} "
        f"--database={database} {opt_table} --project={project} "
        f"{opt_parallel} {opt_async} {opt_quiet} --format=json"
    )

    return backup_command

def exec_sql_export(project_name,instance_name,database,table=""):
    """
        this function is execute command sql export
        and return operation logs 
    """
    command = ""
    base_path = get_archive_path(project_name,instance_name,database)
    if table == "-" or table == "":
        table = ""
        command = backup_command(project_name,instance_name,database,table,base_path[0]['dbengine'],base_path[0]['gcs_archive_path'],base_path[0]['parallel'])
        
    else:
        table = table
        command = backup_command(project_name,instance_name,database,table,base_path[0]['dbengine'],base_path[0]['gcs_archive_path'],base_path[0]['parallel'])

    # command = backup_command(project_name,instance_name,database,table,base_path[0]['gcs_archive_path'],threads)
    print(command)
    cmd_export = shlex.split(command)
    cmd_export_output = subprocess.run(cmd_export,capture_output=True)
    cmd_export_output = json.loads(cmd_export_output.stdout)

    return cmd_export_output

def get_instances_from_backup_config(project_name,instance_name):
    """
        this function is get all instances
        from tbackup_config where 
        crontab is match with currrent datetime
    """
    # set variable DB
    userdb = os.environ.get("USER_DB_DISCOVERY")
    passdb = os.environ.get("PASS_DB_DISCOVERY")
    hostdb = os.environ.get("HOST_DB_DISCOVERY")
    portdb = os.environ.get("PORT_DB_DISCOVERY")
    dbname = os.environ.get("DB_NAME")
    print(project_name,instance_name)

    database_url = f"mysql+mysqlconnector://{userdb}:{passdb}@{hostdb}:{portdb}/{dbname}"
    list_instance =[]

    try:
        engine = create_engine(database_url)
        conn = engine.connect()
        metadata = MetaData()
        tbackup_config = Table('tbackup_config',metadata,autoload_with=engine)
        tdbackup_config = Table('tdbackup_config',metadata,autoload_with=engine)
        statement = select(tbackup_config.c.project_name,\
                           tbackup_config.c.instance_prod,\
                           tdbackup_config.c.instance_sandbox,\
                           tdbackup_config.c.dbname,\
                           tbackup_config.c.param_cron,
                           tdbackup_config.c.tablebackup,
                           tdbackup_config.c.gcs_archive_path,
                           tbackup_config.c.dbengine).\
                    select_from(tbackup_config).join(tdbackup_config, tbackup_config.c.instance_sandbox == tdbackup_config.c.instance_sandbox).\
                    where(tbackup_config.c.project_name == f"{project_name}").\
                    where(tbackup_config.c.instance_sandbox == f"{instance_name}")
        with conn as dbconnection:
            for row in dbconnection.execute(statement):
                list_instance.append(dict(row._mapping))
    except SQLAlchemyError as e:
        print(f"Database Error {e}")
    except Exception as e:
        print(f"instance base on cron - {e}")
        exit();

    return list_instance

    





def submitjob_backup(project_name,instance_name,dbclientname,listtable=None):
    """
        this function is submit job to table job
    """
     # set variable DB
    userdb = os.environ.get("USER_DB_DISCOVERY")
    passdb = os.environ.get("PASS_DB_DISCOVERY")
    hostdb = os.environ.get("HOST_DB_DISCOVERY")
    portdb = os.environ.get("PORT_DB_DISCOVERY")
    alias = os.environ.get("ALIAS")
    dbname = os.environ.get("DB_NAME")

    database_url = f"mysql+mysqlconnector://{userdb}:{passdb}@{hostdb}:{portdb}/{dbname}"

    try: 
        engine = create_engine(database_url)
        conn = engine.connect()
        metadata = MetaData()

        # define table
        tthjob = Table('tthjob',metadata,autoload_with=engine)
        ttdjob = Table('ttdjob',metadata,autoload_with=engine)

        statement = insert(tthjob).values(job_type=2,job_status=0,created_by='friday',updated_by='friday')

        # execute insert tthjob
        result = conn.execute(statement)
        jobheader_id = result.inserted_primary_key[0]

        # insert ttdjob
        if listtable is None:
            #define detail
            task = f"BACKUP FULL BUNDLE DATABASE|{project_name}|{instance_name}"
            detail = f"{dbclientname}|-|- "
            status = 0

            stmt2 = insert(ttdjob).\
                    values(job_header_id=jobheader_id,\
                    job_header_object=task,\
                    job_detail_object=detail,\
                    job_status=status,\
                    created_by = alias, updated_by = alias)
            conn.execute(stmt2)
            conn.commit()
        else:
            # insert detail base on listtable
            for row in listtable:
                task = f"BACKUP DATABASE EACH TABLE|{project_name}|{instance_name}"
                detail = f"{dbclientname}|-|{row['table_name']}"
                status = 0
                stmt2 = insert(ttdjob).\
                        values(job_header_id=jobheader_id,\
                        job_header_object=task,\
                        job_detail_object=detail,\
                        job_status=status,\
                        created_by = alias, updated_by = alias)
                conn.execute(stmt2)
                conn.commit()

    except SQLAlchemyError as e:
        print(f"database Error occured: {e}")
    except Exception as e:
        print(f"An error occured: {e}")


def submit_final_log(project_name, instance_name,database_name,status):
    """
    this function is put final log on table tbackup_log
    """
    userdb = os.environ.get("USER_DB_DISCOVERY")
    passdb = os.environ.get("PASS_DB_DISCOVERY")
    hostdb = os.environ.get("HOST_DB_DISCOVERY")
    portdb = os.environ.get("PORT_DB_DISCOVERY")
    alias = os.environ.get("ALIAS")
    dbname = os.environ.get("DB_NAME")
    dbengine = get_backup_type(project_name,instance_name,database_name)[0]['dbengine']
    parallel = get_backup_type(project_name,instance_name,database_name)[0]['parallel']
    path = f"{get_backup_type(project_name,instance_name,database_name)[0]['gcs_archive_path']}/{create_path(project_name,database_name,dbengine,parallel,"")}"
    backup_type = get_backup_type(project_name,instance_name,database_name)[0]['tablebackup'] 
    if backup_type == 1:
        backup_type = "FULL EACH TABLE"
    else:
        backup_type = "FULL BUNDLE DATABASE"
    instance_name = get_backup_type(project_name,instance_name,database_name)[0]['instance_prod'] 


    database_url = f"mysql+mysqlconnector://{userdb}:{passdb}@{hostdb}:{portdb}/{dbname}"

    try: 
        engine = create_engine(database_url)
        conn = engine.connect()
        metadata = MetaData()

        # define table
        tbackup_log = Table('tbackup_log',metadata,autoload_with=engine)
        
        statement = insert(tbackup_log).\
            values(project_name=project_name,\
                   instance_name=instance_name,\
                   database_name=database_name,\
                   backup_type=backup_type,\
                   filename=path,\
                   path=path,\
                   status=status)
        conn.execute(statement)
        conn.commit()
        print(f"Successfull Insert Backup Log {project_name}-{instance_name}-{database_name}")
        notif("success","Backup Database",project_name,database_name,instance_name)
    except SQLAlchemyError as e:
        print(f"database Error occured: {e}")
        notif("failed","Backup Database",project_name,database_name,instance_name)
    except Exception as e:
        print(f"An error occured: {e}")
        notif("failed","Backup Database",project_name,database_name,instance_name)

    





def main(project_name,instance_name):
    """
        this function is execute backup 
        even full bundle database or per table
    """
    #get all instances from table tbackup_config
    instances = get_instances_from_backup_config(project_name,instance_name)
    print(instances)
    try:
        for row in instances:
            # get tablebackup enabled or disabled
            project_name = row['project_name']
            instances_sandbox = row['instance_sandbox']
            tablebackup = row['tablebackup']
            dbname = row['dbname']
            dbengine = row['dbengine']
            print(tablebackup)

            #check table backup is True
            if tablebackup == 1 :
                #get ip and port
                instance_info = get_ip_machine(project_name,instances_sandbox)

                #get all table
                listtable = get_all_table(dbname,instance_info['ipaddress'],instance_info['port'],dbengine)

                #submitjob
                submitjob_backup(project_name,instances_sandbox,dbname,listtable)

                #send notif
                notif("wip","Submit Job Backup Database",project_name,dbname,instance_name)

            else:
                instance_info = get_ip_machine(project_name,instances_sandbox)
                #send notification
                submitjob_backup(project_name,instances_sandbox,dbname)
                notif("wip","Submit Job Backup Database",project_name,dbname,instance_name)
                # print(instance_info)
    except Exception as e:
        print(f"backup engine - main {e}")
        notif("failed","Submit Job Backup Database",project_name,dbname,instance_name)

if __name__ == "__main__":
    # get_instances_from_backup_config('dbops-dev','mysql-ee88y-sandbox-backup')
    main('dbops-dev','mysql-ee88y-sandbox-backup')
    # exec_sql_export('-dev','mysql-test-sandbox-backup','bill_payment',"-")

