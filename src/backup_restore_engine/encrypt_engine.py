import json
import os
from datetime import date, datetime, timedelta
from sqlalchemy import  create_engine, insert,Table,MetaData,select,Join
from sqlalchemy.exc import SQLAlchemyError
import subprocess
import shlex
from .alltable import get_ip_machine,get_all_table
from dotenv import load_dotenv


#load env file
load_dotenv()

def get_db_connection():
    userdb = os.environ.get("USER_DB_DISCOVERY")
    passdb = os.environ.get("PASS_DB_DISCOVERY")
    hostdb = os.environ.get("HOST_DB_DISCOVERY")
    portdb = os.environ.get("PORT_DB_DISCOVERY")
    dbname = os.environ.get("DB_NAME")
    database_url = f"mysql+mysqlconnector://{userdb}:{passdb}@{hostdb}:{portdb}/{dbname}"
    engine = create_engine(database_url, pool_pre_ping=True)
    return engine


def set_paths(project_name,database_name,typepath,table_name=None,prod_base_paths=None):
    """
    this function set paths base YTD
    and database name to set download
    """
    now = datetime.now()
    temp_base_paths = os.environ.get("GCS_TEMP_PATH")
    yearpath = f"{now.year}"
    monthpath = f"{now.month:02d}"
    # daypath = f"{now.day:02d}"
    daypath = f"17"
    fullday_prefix = f"{yearpath}{monthpath}{daypath}"

    if typepath == 'download':
        set_paths = f"{temp_base_paths}/{project_name}/{database_name}/{yearpath}/{monthpath}/{fullday_prefix}"
        return set_paths
    elif typepath == 'upload':
        set_paths = f"{prod_base_paths}/{project_name}/{database_name}/{yearpath}/{monthpath}/{fullday_prefix}"
        return prod_base_paths
    else:
        print('Type None')



def download_backup(source_paths,destination_paths,project_name,checksums_flag=None):
    """
    This functions is get all backup file
    from gcs and put on the local storage
    """
    base_command = os.environ.get("GCLOUD_COMMAND_RSYNC")
    option = f"--continue-on-error -r"
    projectname = f"--project={project_name}"
    if checksums_flag == None:
        checksums_flag = ""
    else:
        checksums_flag = f"--checksums-only"
    
    fullcommand = (f"{base_command} {source_paths} {destination_paths} "
                f"{option} "
                f"{checksums_flag} "
                f"{projectname} ")
    
    try:
        print(fullcommand)
        cmd_download = shlex.split(fullcommand)
        cmd_download_output = subprocess.run(cmd_download,capture_output=True)
        cmd_download_output = json.loads(cmd_download_output.stdout)
        return cmd_download_output
    except Exception as e:
        print(f"An Error Occured [download_backup] {e}")
        
    print(fullcommand)
    

    # gcloud storage rsync <gs://path> <jumphost/directory> --continue-on-error -r 
    # gcloud storage rsync <gs://path> <jumphost/directory> --continue-on-error -r --checksums-only



def upload_backup():
    # gcloud storage rsync <jumphost/directory> <gs://path> --continue-on-error -r
    # gcloud storage rsync <jumphost/directory> <gs://path> --continue-on-error -r --checksums-only
    pass


def run_encrypt():
    pass



def submit_job(project_name,dbname):
    """
    submit job to table tthjob n ttdjob
    for download, encrypt & upload process

    """
    engine = get_db_connection()
    metadata = MetaData()        
    
    # define table
    tthjob = Table('tthjob',metadata,autoload_with=engine)
    ttdjob = Table('ttdjob',metadata,autoload_with=engine)

    statement = insert(tthjob).values(job_type=3,job_status=1,created_by='friday',updated_by='friday')

    # execute insert tthjob
    # project_name = 'dbops-dev'
    # dbname = 'bill_payment'
    alias = os.environ.get("ALIAS")
    base_command = os.environ.get("GCLOUD_COMMAND_LS")
    source_paths = set_paths('dbops-dev','bill_payment','download')
    get_backup_command = f"{base_command} {source_paths} --project={project_name}"
    base_downloadpath = f"{os.path.dirname(os.path.abspath(__file__))}/{os.environ.get("GCE_PATH")}"
    base_enc_path = f"{os.path.dirname(os.path.abspath(__file__))}/{os.environ.get("GCE_ENC_PATH")}"


    # list all folder from gcs temp
    try:        
        cmd_getbackup = shlex.split(get_backup_command)
        cmd_getbackup_output = subprocess.run(cmd_getbackup,capture_output=True).stdout
        cmd_getbackup_output = cmd_getbackup_output.split(b'\n')
    except Exception as e:
        print(f"An error occured [main] {e}")



    with engine.connect() as dbconnection:
        result = dbconnection.execute(statement)
        jobheader_id = result.inserted_primary_key[0]
        task = f"ENCRYPT UPLOAD|{dbname}" 
        status = 0
        for tablepath in cmd_getbackup_output:
            if not tablepath:  # Skip empty lines
                continue
            relative_path = '/'.join(tablepath.decode('utf-8').split('/')[6:-1])
            downloadpath = os.path.join(base_downloadpath, relative_path)
            detail = f"{tablepath.decode('utf-8')}|{base_downloadpath}|{base_enc_path}"
            # insert ttdjob
            stmt2 = insert(ttdjob).\
                    values(job_header_id=jobheader_id,\
                    job_header_object=task,\
                    job_detail_object=detail,\
                    job_status=status,\
                    created_by = alias, updated_by = alias)
            dbconnection.execute(stmt2)
            dbconnection.commit()
    


def main():
    """
    download all backup from temp path
    and encrypt the backup 
    and then upload the backup to prod GCS
    """

    # get all backup from temporary path
    project_name = 'dbops-dev'
    base_command = os.environ.get("GCLOUD_COMMAND_LS")
    source_paths = set_paths('dbops-dev','bill_payment','download')
    get_backup_command = f"{base_command} {source_paths} --project={project_name}"
    base_downloadpath = f"{os.path.dirname(os.path.abspath(__file__))}/{os.environ.get("GCE_PATH")}"


    # exec command get all backup
    try:        
        cmd_getbackup = shlex.split(get_backup_command)
        cmd_getbackup_output = subprocess.run(cmd_getbackup,capture_output=True).stdout
        cmd_getbackup_output = cmd_getbackup_output.split(b'\n')
    except Exception as e:
        print(f"An error occured [main] {e}")
    
    for tablepath in cmd_getbackup_output:
        if not tablepath:  # Skip empty lines
            continue
        relative_path = '/'.join(tablepath.decode('utf-8').split('/')[6:-1])
        downloadpath = os.path.join(base_downloadpath, relative_path)
        print(downloadpath)
        download_backup(tablepath.decode('utf-8'),downloadpath,'dbops-dev')


if __name__ == '__main__':
    submit_job()