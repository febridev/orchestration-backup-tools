import json 
import os
import subprocess
import shlex
from sqlalchemy import Table,select,create_engine,MetaData,insert
from dotenv import load_dotenv
from .notification import create_message as notif

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
    filter = f"--filter='name:*{prefix_instance}'"
    format = f"--format='json(name,serviceAccountEmailAddress,project)'"
    project_name = f"--project={project_name}"
    full_command = (f"{base_command} "
                    f" {project_name}"
                    f" {filter}"
                    f" {format}"
                    )
    #execute command
    try:
        cmd_list = shlex.split(full_command)
        cmd_list_output = subprocess.run(cmd_list,capture_output=True)
        cmd_list_output = json.loads(cmd_list_output.stdout)

        for row in cmd_list_output:
            instance_sandbox_backup.append(row)
    except Exception as e:
        print(f"patch-instance - {e}")
        exit()

    return instance_sandbox_backup

def exec_grant(project_name,instance_name,service_account,gcs=""):
    """
        this function for exect grant cloudsql sandbox to 
        existing gcs for read and write
    """
    base_command = os.environ.get("GCLOUD_COMMAND_GRANT")
    if gcs == "":
        gcs_path = os.environ.get("GCS_BASE")
    else:
        gcs_path = gcs
    gcs_roles = f"--role={os.environ.get("GCS_ROLES")}"
    service_account = f"--member=serviceAccount:{service_account}"
    
    grant_command = (
        f"{base_command}"
        f" {gcs_path}"
        f" {service_account}"
        f" {gcs_roles}"
        f" --format=json --quiet"
    )
    print(grant_command)

    # exec command 
    try:
        cmd_grant = shlex.split(grant_command)
        cmd_grant_output = subprocess.run(cmd_grant,capture_output=True)
        cmd_grant_output = json.loads(cmd_grant_output.stdout)
    except Exception as e:
        print(f"exec_grant - {e}")
        exit()

    return cmd_grant_output

def main(project_name,instance_name):
    """
        this function is execute remove flag high avaibility and delete protection
        on instance sandbox-backup before execute cloud sql export
    """
    # set variable DB
    userdb = os.environ.get("USER_DB_DISCOVERY")
    passdb = os.environ.get("PASS_DB_DISCOVERY")
    hostdb = os.environ.get("HOST_DB_DISCOVERY")
    portdb = os.environ.get("PORT_DB_DISCOVERY")
    dbname = os.environ.get("DB_NAME")
    database_url = f"mysql+mysqlconnector://{userdb}:{passdb}@{hostdb}:{portdb}/{dbname}"


    list_sandbox_backup = []

    # Execute Get Clone Instnaces
    try:
        engine = create_engine(database_url)
        conn = engine.connect()
        metadata = MetaData()
        tbackup_config = Table('tbackup_config',metadata,autoload_with=engine)
        tdbackup_config = Table('tdbackup_config',metadata,autoload_with=engine)
        stmt = select(tbackup_config.c.project_name,tbackup_config.c.instance_sandbox, tdbackup_config.c.gcs_archive_path).\
            select_from(tbackup_config).join(tdbackup_config, tbackup_config.c.instance_sandbox == tdbackup_config.c.instance_sandbox).\
            where(tbackup_config.c.project_name == f"{project_name}").\
            where(tbackup_config.c.instance_sandbox == f"{instance_name}").\
            where(tbackup_config.c.backup_status == "enabled")
        
        with conn:
            for row in conn.execute(stmt):
                instance_sandbox = get_clone_instance(row._mapping['project_name'])
                for rinstance in instance_sandbox:
                    list_sandbox_backup.append(dict(project_name=row._mapping['project_name'],instance_name=rinstance,gcs_archive_path=row._mapping['gcs_archive_path']))



    except Exception as e:
        print(f"main patch instance - {e}")
        exit()

    # Execute Submit Job Patch & Execute Patch
    # 
    try:
        engine = create_engine(database_url)
        conn = engine.connect()
        metadata = MetaData()
        tthjob = Table('tthjob',metadata,autoload_with=engine)
        ttdjob = Table('ttdjob',metadata,autoload_with=engine)

        #insert header job
        stmtheader = insert(tthjob).values(job_type=7,job_status=2,created_by='friday',updated_by='friday')

        # execute insert tthjob
        result = conn.execute(stmtheader)
        jobheader_id = result.inserted_primary_key[0]


        for rows in list_sandbox_backup:
            project_name = rows['project_name']
            gcs_archive_path = rows['gcs_archive_path']
            instance_name = rows['instance_name']['name']
            service_account = rows['instance_name']['serviceAccountEmailAddress']
            task = f"grant_instance_gcs|{project_name}|{instance_name}"
            status = 2
            alias = os.environ.get("ALIAS")
            run_command = exec_grant(project_name,instance_name,service_account,gcs_archive_path)
            operation_name = os.environ.get("GCS_ROLES")
            operation_type = f"{service_account}"
            detail = f"{operation_type}|{operation_name}|{instance_name}" 
            stmtdetail = insert(ttdjob).\
                values(job_header_id=jobheader_id,\
                       job_header_object=task,\
                       job_detail_object=detail,\
                       job_status=status,\
                       created_by = alias, updated_by = alias)
            print(stmtdetail)
            conn.execute(stmtdetail)
        conn.commit()
        conn.close()

    except Exception as e:
        print(f"exec patch instance - {e}")
        # send notification
        notif("failed","Grant Instance To GCS",project_name,"-",instance_name)

        # exit()
    return



if __name__ == '__main__':
    main('dbops-dev','postgres-xxew1-sandbox-backup')

