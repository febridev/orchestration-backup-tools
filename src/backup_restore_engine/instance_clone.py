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
from .notification import create_message as notif
from dotenv import load_dotenv


#load enviroment variable
load_dotenv()
def get_clone_instance(project_name, instance_name=""):
    """
        this function get all clone instance with prefix 'sandbox-backup'
        base on projectname and return as json 
    """
    instance_sandbox_backup = []
    #setup command 
    prefix_instance = os.environ.get("CLOUDSQL_PREFIX")
    base_command = os.environ.get("GCLOUD_COMMAND_INSTANCES")
    # filter = f"--filter='name:*{prefix_instance}'"
    filter = f"--filter='name:{instance_name}'"
    project_name = f"--project={project_name}"
    full_command = (f"{base_command} "
                    f" {project_name}"
                    f" {filter} --format=json"
                    )
    #execute command
    try:
        cmd_list = shlex.split(full_command)
        cmd_list_output = subprocess.run(cmd_list,capture_output=True)
        cmd_list_output = json.loads(cmd_list_output.stdout)

        for row in cmd_list_output:
            instance_sandbox_backup.append(row['name'])
            
        return instance_sandbox_backup
    except subprocess.CalledProcessError as e:
        print(f"Error executing gcloud command: {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"Error parsing gcloud command output: {e}")
        raise
    except Exception as e:
        print(f"patch-instance - {e}")
        exit()




def clone_command(project,sources_instances,destination_instances,point_in_time=None):
    """
        this function is for generate gcloud command
        for clone instances from production instances
        with prefix 'sandbox-backup'
    """
    #setup command gcloud clone 
    clone_base_cmd = os.environ.get("GCLOUD_COMMAND_CLONE")
    prefix = os.environ.get("CLOUDSQL_PREFIX")

    # Validate required parameters
    if not project:
        raise ValueError("Project id is required")
    if not sources_instances:
        raise ValueError("Source instance name is required.")
    if not destination_instances:
        raise ValueError("Destination instance name is required.")
    if destination_instances.startswith(prefix):
        raise ValueError("Destination instance doesn't sandbox-backup")

    # Optional point-in-time parameter
    if point_in_time != None:
        # parse string point_in_time
        dt = datetime.strptime(point_in_time, "%Y-%m-%d %H:%M:%S.%f")
        
        # Set timezone to UTC+7
        dt = dt.replace(tzinfo=ZoneInfo("Asia/Jakarta"))
        
        # conversion to UTC
        dt_utc = dt.astimezone(ZoneInfo("UTC"))

        point_in_time = f"--point-in-time={dt_utc}"
    else:
        point_in_time = ""


    clone_command = (
        f"{clone_base_cmd} {sources_instances} {destination_instances}"
        f" {point_in_time} --project={project}"
        f" --format=json --async --quiet"
    )

    return clone_command


def get_instances(project_name,instances_name=""):
    """
        this function is get all instances from database_discovery
        base on project name as required and instances namae as Optional
    """
        # set variable DB
    userdb = os.environ.get("USER_DB_DISCOVERY")
    passdb = os.environ.get("PASS_DB_DISCOVERY")
    hostdb = os.environ.get("HOST_DB_DISCOVERY")
    portdb = os.environ.get("PORT_DB_DISCOVERY")
    dbname = os.environ.get("DB_NAME")

    database_url = f"mysql+mysqlconnector://{userdb}:{passdb}@{hostdb}:{portdb}/{dbname}"


    list_instance =[]
    statement = ""


    # get instances base on project_name or
    try:
        engine = create_engine(database_url)
        conn = engine.connect()
        metadata = MetaData()

        # tmproject = Table('tmproject',metadata,autoload_with=engine)
        # tthinstance = Table('tthinstance',metadata,autoload_with=engine)
        tbackup_config = Table('tbackup_config',metadata,autoload_with=engine)

        if instances_name == "":
            statement = select(tbackup_config.c.instance_prod, tbackup_config.c.instance_sandbox).\
            select_from(tbackup_config).\
            where(tbackup_config.c.project_name == f"{project_name}")
            # statement = select(tmproject.columns.project_name,tthinstance.columns.instance_name).\
            # select_from(tmproject).join(tthinstance, tmproject.c.seq_id == tthinstance.c.project_id).\
            # where(tmproject.c.project_name == f"{project_name}")
        else:
            statement = select(tbackup_config.c.instance_prod, tbackup_config.c.instance_sandbox).\
            select_from(tbackup_config).\
            where(tbackup_config.c.project_name == f"{project_name}").\
            where(tbackup_config.c.instance_prod == f"{instances_name}")

            # statement = select(tmproject.columns.project_name,tthinstance.columns.instance_name).\
            # select_from(tmproject).join(tthinstance, tmproject.c.seq_id == tthinstance.c.project_id).\
            # where(tmproject.c.project_name == f"{project_name}").\
            # where(tthinstance.c.instance_name == f"{instances_name}")



        with conn as dbconnection:
            for row in dbconnection.execute(statement):
                list_instance.append(dict(row._mapping))

    except Exception as e:
        print(f"get_instances - {e}")
    return list_instance




def exec_clone(command):
    """
        this function is execute command from return clone command
        and executed it to create clone
    """
    cmd_clone = shlex.split(command)
    cmd_clone_output = subprocess.run(cmd_clone,capture_output=True)
    cmd_clone_output = json.loads(cmd_clone_output.stdout)

    return cmd_clone_output




def main(project_name,instance_name):
    """
        this main function is orchrestator on instance_clone.py
    """
    operation_log = []
    tbl1 = ""
    msg = ""
    run_command=""
    sql2 = ""

    # set variable DB

    userdb = os.environ.get("USER_DB_DISCOVERY")
    passdb = os.environ.get("PASS_DB_DISCOVERY")
    hostdb = os.environ.get("HOST_DB_DISCOVERY")
    portdb = os.environ.get("PORT_DB_DISCOVERY")
    dbname = os.environ.get("DB_NAME")

    database_url = f"mysql+mysqlconnector://{userdb}:{passdb}@{hostdb}:{portdb}/{dbname}"

    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)


    # insert header job and detail job
    try:
        engine = create_engine(database_url)
        conn = engine.connect()
        metadata = MetaData()

        # define table
        tthjob = Table('tthjob',metadata,autoload_with=engine)
        ttdjob = Table('ttdjob',metadata,autoload_with=engine)

        # statement = text("""INSERT INTO tthjob (job_type,job_status,created_by,updated_by) VALUES (1,0,'friday','friday');""")
        statement = insert(tthjob).values(job_type=1,job_status=0,created_by='friday',updated_by='friday')

        # execute insert tthjob
        result = conn.execute(statement)
        jobheader_id = result.inserted_primary_key[0]

        # insert job detail 
        # get instances
        lsinstances = get_instances(project_name,instance_name)
        
        for rows in lsinstances: 
            # project_name = rows['project_name']
            project_name = project_name
            instance_name = rows['instance_prod']
            task = f"instance_clone|{project_name}|{instance_name}"
            status = 0
            alias = os.environ.get("ALIAS")
            # dest_instance = f"{instance_name}-sandbox-backup"
            dest_instance = rows['instance_sandbox']
            existing_instances = get_clone_instance(project_name,instance_name)
            if dest_instance in existing_instances:
                logger.warning(f"Instance {dest_instance} already exists. Skipping.")
                continue

            print(clone_command(project_name,instance_name,dest_instance))
            run_command = exec_clone(clone_command(project_name,instance_name,dest_instance))
            operation_name = run_command['name']
            operation_type = run_command['operationType']
            detail = f"{operation_type}|{operation_name}|{dest_instance}" 
                
            sql2 = insert(ttdjob).\
                values(job_header_id=jobheader_id,\
                    job_header_object=task,\
                    job_detail_object=detail,\
                    job_status=status,\
                    created_by = alias, updated_by = alias)
            # print(sql2)
            conn.execute(sql2)



        #commit header job and detail job
        conn.commit()

        #close connection
        conn.close()

        # submit message
        msg = {f"Job Clone CloudSQL Instances":"Success - Job Already Submitted Please Check Job Detail"}
    except SQLAlchemyError as e:
        print (f"database Error occured: {e}")
    except Exception as e:
        print(f"something error - {e}")
        # send notification
        notif("failed","Clone Instance",project_name,'-',instance_name)
        # exit()
        

    return msg





if __name__=="__main__":
    # main("dbops-dev")
    get_clone_instance('dbops-dev','postgres-xxew1-sandbox-backup')