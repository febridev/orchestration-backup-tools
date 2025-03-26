import json
import os
from sqlalchemy import  create_engine, Table,MetaData,select,Join,text
import subprocess
import shlex
from .instance_clone import get_instances
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

load_dotenv()



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
        dbname = 'information_schema'
        statement = text(f"""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema = :dbname
        """)
    elif enginedb == 'Postgres':
        connector = "postgresql+psycopg2"
        dbname = client_dbname
        statement = text(f"""
            SELECT table_schema, table_name 
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

def main(client_dbname,ipdb,portdb,enginedb):
    get_all_table(client_dbname,ipdb,portdb,enginedb)


# if __name__ == "__main__":
    # info_instance = get_ip_machine('dbops-dev','dbops-d-cloudsql-mysql-test')
    # get_all_table('bill_payment',info_instance['ipaddress'],info_instance['port'])

    
