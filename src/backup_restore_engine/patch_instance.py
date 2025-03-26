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
    except Exception as e:
        print(f"patch-instance - {e}")
        exit()

    print(instance_sandbox_backup)
    return instance_sandbox_backup

def exec_patch(project_name,instance_name):
    """
        this function for exec patch command remove flag high avaibility zone and delete protection
        for sandbox backup instance
    """
    base_command = os.environ.get("GCLOUD_COMMAND_PATCH")
    patch_delete_protection = "--no-deletion-protection"
    patch_single_zone = "--availability-type=zonal"
    project_name = f"--project={project_name}"
    
    patch_command = (
        f"{base_command}"
        f" {instance_name}"
        f" {patch_delete_protection} {patch_single_zone}"
        f" {project_name}"
        f" --format=json --async --quiet"
    )
    print(patch_command)

    # exec command 
    try:

        cmd_patch = shlex.split(patch_command)
        cmd_patch_output = subprocess.run(cmd_patch,capture_output=True)
        cmd_patch_output = json.loads(cmd_patch_output.stdout)
    except Exception as e:
        print(f"exec_patch - {e}")
        exit()

    return cmd_patch_output

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
        stmt = select(tbackup_config.c.project_name,tbackup_config.c.instance_sandbox).\
            select_from(tbackup_config).\
            where(tbackup_config.c.project_name == f"{project_name}").\
            where(tbackup_config.c.instance_sandbox == f"{instance_name}").\
            where(tbackup_config.c.backup_status == "enabled")
        
        with conn:
            for row in conn.execute(stmt):
                instance_sandbox = get_clone_instance(row._mapping['project_name'],row._mapping['instance_sandbox'])
                for rinstance in instance_sandbox:
                    list_sandbox_backup.append(dict(project_name=row._mapping['project_name'],instance_name=rinstance))

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
        stmtheader = insert(tthjob).values(job_type=6,job_status=0,created_by='friday',updated_by='friday')

        # execute insert tthjob
        result = conn.execute(stmtheader)
        jobheader_id = result.inserted_primary_key[0]


        for rows in list_sandbox_backup:
            project_name = rows['project_name']
            instance_name = rows['instance_name']
            task = f"patch_instance|{project_name}|{instance_name}"
            status = 0
            alias = os.environ.get("ALIAS")
            run_command = exec_patch(project_name,instance_name)
            operation_name = run_command['name']
            operation_type = f"{run_command['operationType']} REMOVE FLAG"
            detail = f"{operation_type}|{operation_name}|{instance_name}" 


            stmtdetail = insert(ttdjob).\
                values(job_header_id=jobheader_id,\
                       job_header_object=task,\
                       job_detail_object=detail,\
                       job_status=status,\
                       created_by = alias, updated_by = alias)
            # print(stmtdetail)
            conn.execute(stmtdetail)
        conn.commit()
        conn.close()

    except Exception as e:
        print(f"exec patch instance - {e}")

        # send notification
        notif("failed","Patch Instance Remove Flag High Avaibility and Delete Protection",project_name,"-",instance_name)
        # exit()


    return "Ok"




if __name__ == '__main__':
    # print(get_clone_instance('dbops-dev'))
    main()
