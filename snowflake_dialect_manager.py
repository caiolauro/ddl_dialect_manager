import os
import re
import csv
import pytz
from datetime import datetime, date, timedelta
import logging
from utils.snowflake.connector.snowflake_connector import initialize_snowflake_connector_using_adp_python_user as initalize_snowflake
if __name__ == '__main__':
    from ddl_diffs import get_ddl_dictionary, get_ddl_tables
    from ddl_helper import DATE_TO_TIMESTAMP_NTZ, TZ, CURRENT_US_PACIFIC_DATE, CURRENT_US_PACIFIC_DATETIME \
    , convert_pk_constraint_string_to_list, write_ddl_row

else:
    from utils.snowflake.schema_change.ddl_diffs import get_ddl_dictionary, get_ddl_tables
    from utils.snowflake.schema_change.ddl_helper import DATE_TO_TIMESTAMP_NTZ, TZ, CURRENT_US_PACIFIC_DATE, CURRENT_US_PACIFIC_DATETIME \
        , convert_pk_constraint_string_to_list, write_ddl_row
    from utils.snowflake.connector.snowflake_connector import SnowflakeConnector, SnowflakeExecutor
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s')
utilLogger = logging.getLogger(__name__)
utilLogger.setLevel(logging.INFO)
file_handler = logging.FileHandler('logs/utils.log')
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
utilLogger.addHandler(file_handler)
file_handler.setFormatter(formatter)


class Paths:
    """Responsible to store and manage the utility paths."""
    partitionTablesCSV = 'utils/snowflake/schema_change/input/metadata/partitioning/tables_to_partition.csv'
    PULL_CLIENT_NAME_FROM_FULL_PATH = lambda x: x.split('/')[-1].split('_')[3].upper()
    PULL_CLIENT_NAME_FROM_FILE_NAME = lambda x: x.split('_')[3].upper()
    @classmethod
    def readPartitionTables(cls):
        with open(cls.partitionTablesCSV,'r') as partitionCSV:
            rows = partitionCSV.read()
            rowsList = rows.split('\n')
            partitionTablesDict = {}
            
            for row in rowsList:
                table_key = row.split(',')[0]+'.'+ row.split(',')[1].strip()
                partitionTablesDict[table_key] = row.split(',')[2].strip()

        return partitionTablesDict  


class SnowflakeDialect:
    """Responsible to store and manage the Snowflake related Syntax."""
    metadataColumns = \
        "\t(\n\tOPERATION_TYPE VARCHAR(1) COMMENT 'Metadata Column',\n\tFULLY_QUALIFIED_TABLE_NAME VARCHAR(100) COMMENT 'Metadata Column',\n\tROW_COMMIT_TIME TIMESTAMP COMMENT 'Metadata Column',\n\tROW_EXTRACT_TIME TIMESTAMP COMMENT 'Metadata Column',\n\tTRAIL_POSITION VARCHAR(100) COMMENT 'Metadata Column',\n\tDATA_INGESTION_TIME TIMESTAMP COMMENT 'Metadata Column',\n"
    useDatabase = "  USE DATABASE {database};\n"
    tableClusterBody = """\tCLUSTER BY (
    SHARED.PUBLIC."GET_TS_PART"({cluster_key_column}, 0, '-', ' ', ' ',':'),
    SHARED.PUBLIC."GET_TS_PART"({cluster_key_column}, 1, '-', ' ', ' ',':'),
    SHARED.PUBLIC."GET_TS_PART"({cluster_key_column}, 2, '-', ' ', ' ',':')
    )\n"""
    tableComment = "\tCOMMENT = 'Environment: {0} ,DDL Extracted at: {1}, DDL Translation Generated at: {2}'"
    pipeBody = """CREATE PIPE IF NOT EXISTS {pipe_fqn} 
                            AUTO_INGEST=FALSE
                            AS
                            COPY INTO {db}.{schema}.{table_name}
                            FROM {s3_stage}
                            FILE_FORMAT = (FORMAT_NAME = {fileFormatFqn}, NULL_IF = ('','NULL','null','"','""'));"""

    operationalPipeBody = """
        CREATE PIPE IF NOT EXISTS {pipe_fqn}
                            AUTO_INGEST=FALSE
                            AS
                            COPY INTO {db_name}.{schema_name}.{table_name}
                            FROM
                            (
                                SELECT
                                    {column_list}
                                FROM
                                    {stage}/ A)
                            FILE_FORMAT = (FORMAT_NAME = {file_format}, NULL_IF = ('','NULL','null'));
    """
    secure_db_tables_template = """
    CREATE TABLE IF NOT EXISTS SECURE_DATA.{schema_name}.{table_name}
    (
	    token_value                TEXT,
	    encryption_key_id          TEXT,    /* Id of the key used to encrypt the data */
	    sensitive_data             TEXT     /* encrypted PCI data */
    );"""
    stageNames = {'PROD':'@SHARED.PUBLIC.PRD_ADP_STAGE/'
                  , 'PROD_OPERATIONAL': '@SHARED.PUBLIC.PRD_OPERATIONAL_EXPORTS_STAGE'
                , 'PERF':'@SHARED.PUBLIC.PERF_ADP_STAGE/'
                , 'PERF_OPERATIONAL': '@SHARED.PUBLIC.PERF_OPERATIONAL_EXPORTS_STAGE'
                }

    copy_history_query_body = """ 
                            {table_name} AS 
                            (select *
                            from table(
                            {db_name}.information_schema.copy_history(
                                table_name=>'{db_name}.{client_name}.{table_name}',
                                start_time=> dateadd(hours, -{past_hours}, current_timestamp())
                            ))),
                         """
    masking_policies = {
                        1:'SHARED.PUBLIC.MASK_VARCHAR_USING_SHA512', # temporary
                        3:'SHARED.PUBLIC.MASK_VARCHAR_USING_SHA512',
                        4:'SHARED.PUBLIC.MASK_TIMESTAMP_NTZ',
                        5:'SHARED.PUBLIC.MASK_NUMBER'
                        }
    snowflake_users_dict = {
        'ORACLE_RAW': {"user_name":"ADP_PYTHON_PROD_USER", "role_name":"ADP_PYTHON_PROD_ROLE", "account":"mha08645.us-east-1", "warehouse":"DATA_INGESTION_WAREHOUSE", "key_path":"keys/adp_python_user_key.p8", "is_sandbox_key":False}, 
        'SANDBOX_SCHEMACHANGE': {"user_name":"SCHEMACHANGE_SYSTEM_TEST_SHARED_USER", "role_name":"SCHEMACHANGE_SYSTEM_TEST_SHARED_ROLE", "account":"GNA62195.US-EAST-1", "warehouse":"DATA_SCIENCE_WAREHOUSE", "key_path":"keys/sandbox_schemachange_private_key.p8", "is_sandbox_key":True},
        'S5PRC2C': {"user_name":"ADP_PYTHON_USER", "role_name":None, "account":"gna62195.us-east-1", "warehouse":"DATA_SCIENCE_WAREHOUSE", "key_path":"keys/rsa_key_perf.p8", "is_sandbox_key":True}
        }
    primary_key_sproc_template = """
    CALL SHARED.PUBLIC.SAFE_ADD_PRIMARY_KEY_CONSTRAINT ('{full_qualified_table_name}', '{constraint_name}', {column_names_array});
    """
    alter_primary_key = """
    ALTER TABLE {full_qualified_table_name} ADD CONSTRAINT {constraint_name} PRIMARY KEY {column_names_array};
    """#SANDBOX_SCHEMACHANGE
    @staticmethod
    def masking_policy_alter_statement(db,schema,table,column,masking_policy_fqn,dtype):
        template =f'-- Column Name: {column} | Column Data Type: {dtype} | Masking Policy Applied: {masking_policy_fqn.split(".")[2]}\nALTER TABLE {db}.{schema}.{table} MODIFY COLUMN {column} SET MASKING POLICY {masking_policy_fqn};'
        return template


class OracleDialect:
    """Responsible to store and manage the Oracle related Syntax."""
    end_of_ddl_file_date = [f"{datetime.now().strftime('%B').upper()[:3]}-{datetime.now().strftime('%y').upper()}"
                           ,f"{(datetime.now() - timedelta(days=30)).strftime('%B').upper()[:3]}-{datetime.now().strftime('%y').upper()}"]
    end_of_file_bad_sql = 'SQL> '


class SnowflakeDialectManager:
    """Translates DDLs in Oracle Dialect to Snowflake Dialect."""
    input_ds_path = 'utils/snowflake/schema_change/input/data_sources' # need to move to path class
    data_sources = os.listdir(input_ds_path) # need to move to path class
    first_dump_ddl_dir_path = 'utils/snowflake/schema_change/output/first_dump_ddls' # need to move to path class
    output_paths_dict = None
    inputDataSourcePathDict = {f"{data_source.upper()}" :f'utils/snowflake/schema_change/input/data_sources/{data_source}/' for data_source in data_sources} # need to move to path class
    outputDataSourcePathDict = {f"{data_source.upper()}" :f'utils/snowflake/schema_change/output/first_dump_ddls/{data_source}/' for data_source in data_sources} # need to move to path class
    def __init__( self
                , databaseName = 'ORACLE_TEST'
                #, incrementBy=1
                #, version = 0
                , data_source='AWS_RDS'
                , data_source_path = None
                , target_clients = None
                ):

                self.databaseName:str                   = databaseName
                #self.__incrementBy:int                  = incrementBy
                #self.__version:int                      = version
                self.data_source:str                    = data_source
                self.data_source_path:str               = self.__get_input_ddl_path(data_source)
                self.data_source_ddl_extracted_at:str   = self.data_source_path.split('/')[-1]
                self.columns_ordinal_position           = {} #{"table_name" : [(0, "FNAME"),(1,"LNAME"),...,(n,"PHONE")]}
                self.is_encrypt_columns_list            = []
                self.input_data_source_file_paths       = []
                self.output_data_source_file_paths      = {}
                self.first_dump_ddl_file_paths          = []
                self.target_clients                     = target_clients
                self.clients_tables                     = None
                self.clients                            = None
                print("Setting Up Dialect Manager Object Requirements...")
                self.__make_first_dump_ddl_dirs()
                self.__make_output_data_sources_dirs()
                self.__get_input_and_output_data_source_file_paths()
                self.__get_first_dump_ddl_file_paths()
                self.__get_clients()
    
    @property
    def version(self):
        return self.__version

    @classmethod
    def __get_input_ddl_path(cls, target_data_source):
        """From 'utils/snowflake/schema_change/input/data_sources/<target_data_source>', selects 
        the most recent data_source folder. Example: 
        |-- input
            |-- data_sources
                |-- cv
                    |--2021-11-24
                    |--2022-03-28 >> function retrieves 'utils/snowflake/schema_change/input/data_sources/cv/2022-03-28'
        """
        ds_path = cls.inputDataSourcePathDict[target_data_source]
        ds_date_folders = os.listdir(ds_path)
        most_recent_date = max([date(int(ds_date.split('-')[0]),int(ds_date.split('-')[1]),int(ds_date.split('-')[2])) for ds_date in ds_date_folders])
        # format datetime to string
        most_recent_date = most_recent_date.strftime('%Y-%m-%d')

        return os.path.join(ds_path, most_recent_date)    
    
    def __get_input_and_output_data_source_file_paths(self):
        output_data_source_path = self.data_source_path.replace('input','output')
        if self.target_clients:
            for input_file in sorted(os.listdir(self.data_source_path), key=Paths.PULL_CLIENT_NAME_FROM_FILE_NAME):
                if Paths.PULL_CLIENT_NAME_FROM_FILE_NAME(input_file) in self.target_clients:
                    input_file_full_path = f'{self.data_source_path}/{input_file}'
                    self.input_data_source_file_paths.append(input_file_full_path)
            
            for output_file in sorted(os.listdir(output_data_source_path), key=Paths.PULL_CLIENT_NAME_FROM_FILE_NAME):
                if Paths.PULL_CLIENT_NAME_FROM_FILE_NAME(output_file) in self.target_clients:
                    output_file_full_path = f'{output_data_source_path}/{output_file}'
                    client_name = Paths.PULL_CLIENT_NAME_FROM_FILE_NAME(output_file)
                    self.output_data_source_file_paths[client_name] = output_file_full_path
       
        else:

            for input_file in sorted(os.listdir(self.data_source_path), key=Paths.PULL_CLIENT_NAME_FROM_FILE_NAME):
                input_file_full_path = f'{self.data_source_path}/{input_file}'
                self.input_data_source_file_paths.append(input_file_full_path)
            for output_file in sorted(os.listdir(output_data_source_path), key=Paths.PULL_CLIENT_NAME_FROM_FILE_NAME):
                    output_file_full_path = f'{output_data_source_path}/{output_file}'
                    client_name = Paths.PULL_CLIENT_NAME_FROM_FILE_NAME(output_file)
                    self.output_data_source_file_paths[client_name] = output_file_full_path

    def __make_first_dump_ddl_dirs(self):
        if not os.path.exists(SnowflakeDialectManager.first_dump_ddl_dir_path):
            os.mkdir(SnowflakeDialectManager.first_dump_ddl_dir_path)
            print(f"Creating Directory: {SnowflakeDialectManager.first_dump_ddl_dir_path}")
        for data_source in SnowflakeDialectManager.outputDataSourcePathDict:
            if not os.path.exists(SnowflakeDialectManager.outputDataSourcePathDict[data_source]):
                os.mkdir(SnowflakeDialectManager.outputDataSourcePathDict[data_source])
                print(f"Creating Directory: {SnowflakeDialectManager.outputDataSourcePathDict[data_source]}")
    
    def __make_output_data_sources_dirs(self,dry_mode=False):
        """For every directory inside 'utils/snowflake/schema_change/input/data_sources'
        this method creates a corresponding directory in utils/snowflake/schema_change/output/data_sources
        example:
        utils/snowflake/schema_change/input/data_sources/aws_rds/2022-05-10
        utils/snowflake/schema_change/input/data_sources/aws_rds/2022-05-10

        *Set dry_mode as True to create the directory
        """
        output_paths_dict = {}
        output_dirs_paths = []
        input_dirs_paths = []
        data_sources_output_paths = [f"{SnowflakeDialectManager.input_ds_path}/{data_source}/".replace('input','output') for data_source in SnowflakeDialectManager.data_sources]
        for opath in data_sources_output_paths:
            data_source = opath.split('/')[5].upper()
            if os.path.exists(opath)==False:
                os.mkdir(opath)
                print(f"Creating Directory: {opath}")
            
            ipath = opath.replace('output','input')
            ds_dates = os.listdir(ipath)
            output_ds_paths_to_dates = [f"{opath}{date}" for date in ds_dates]
            input_ds_paths_to_dates = [f"{ipath}{date}" for date in ds_dates]
            output_paths_dict[data_source] = output_ds_paths_to_dates
            for ds_path_to_date in output_ds_paths_to_dates:
                if os.path.exists(ds_path_to_date)==False and dry_mode==False:
                    os.mkdir(ds_path_to_date)
                    print(f"Creating Directory: {ds_path_to_date}")
                output_dirs_paths.append(ds_path_to_date)

        return output_paths_dict
    
    def __get_first_dump_ddl_file_paths(self):

        for file in self.input_data_source_file_paths:
            core_name = file.split('/')[7].split('_')[3].upper() #'utils/snowflake/schema_change/input/data_sources/oracle_oltp/2022-01-14/tab_ddl_x1prc10p_aspiration_2022-01-14T12_30_10-0700.lst'
            if self.target_clients and core_name not in self.target_clients: 
                continue
            cores_startswith = core_name[0].upper()
            group_path = f'{SnowflakeDialectManager.outputDataSourcePathDict[self.data_source]}/{cores_startswith}'
            client_output_dir = f'{SnowflakeDialectManager.outputDataSourcePathDict[self.data_source]}{cores_startswith}/{core_name}'
            if not os.path.exists(group_path) : 
                group_path = os.path.join(SnowflakeDialectManager.outputDataSourcePathDict[self.data_source],cores_startswith)
                os.mkdir(group_path)
            if not os.path.exists(client_output_dir) : 
                os.mkdir(client_output_dir)
            fname = f"{core_name}_{CURRENT_US_PACIFIC_DATE}_a_create_table.sql"

            if len(os.listdir(client_output_dir)) > 0:
                for ofile in os.listdir(client_output_dir):
                    self.first_dump_ddl_file_paths.append(f'{client_output_dir}/{ofile}')
            else:
                    self.first_dump_ddl_file_paths.append(f'{client_output_dir}/{fname}')

    def __get_clients(self):
        clients = [Paths.PULL_CLIENT_NAME_FROM_FULL_PATH(self.output_data_source_file_paths[client]) for client in self.output_data_source_file_paths]
        self.clients = clients
        print("Clients List successfully generated.")

    def initialize_snowflake_connection(self):
        creds = SnowflakeDialect.snowflake_users_dict[self.databaseName]
        snow_connection = SnowflakeConnector(
            creds["user_name"],
            creds["role_name"],
            creds["account"],
            creds["warehouse"],
            creds["key_path"],
            creds["is_sandbox_key"]
            )
        snow_connection.initialize()
        return snow_connection.cursor, snow_connection.con

    def get_clients_tables(self):
        tables_dict = {}
        for client in self.output_data_source_file_paths:
            tables_dict[client] = []
            client = Paths.PULL_CLIENT_NAME_FROM_FULL_PATH(self.output_data_source_file_paths[client])
            client_ddl_dictionary_list = get_ddl_dictionary(self.output_data_source_file_paths[client])
            for table_key in client_ddl_dictionary_list:
                table_name = table_key.split('.')[1]
                tables_dict[client].append(table_name)
            
        self.clients_tables = tables_dict      
    
    def generate_first_dump_tables(self, tables_filter=None, tables_filter_exclude=None, include_clustering_keys=None, sproc_mode=False):
        """
        -> Translates Tables Oracle DDL to Snowflake DDL
        -> Adds Comment to each table
        -> Adds Metadata Columns
        -> Adds Clustering to Tables with size > 1TB
        -> Writes DDL SQL File for each Client
        """
        ############## Logging 
        utilLogger.info(msg=f"Parsing {str(len(self.input_data_source_file_paths))} DDL file(s) to Snowflake Syntax... | From Data Source: {self.data_source}")
        year,month,day = [int(d) for d in self.data_source_ddl_extracted_at.split('-')]
        days_with_no_update_on_ddl_extract = (date.today() -  date(year,month,day)).days 
        if days_with_no_update_on_ddl_extract > 15:
                utilLogger.warning(msg=f"Check with DBA team if there's a fresher version of DDL's. Source is from {days_with_no_update_on_ddl_extract} day(s)")
        ############## Declaring Variables
        clients_count = 0
        total_tables = 0
        i = 0
        ############## Get PCI Information
        cursor, conn = self.initialize_snowflake_connection()
        snowflake_query_executor = SnowflakeExecutor(cursor, conn)
        token_columns_key_list = snowflake_query_executor.get_tokenized_columns()
        print(token_columns_key_list)
        ############## Start Files I/O 
        for ddl_input_file_path, ddl_output_file_path  in zip(self.input_data_source_file_paths,self.first_dump_ddl_file_paths):       
            
            core_name = ddl_input_file_path.split('/')[7].split('_')[3].upper() #'utils/snowflake/schema_change/input/data_sources/oracle_oltp/2022-01-14/tab_ddl_x1prc10p_aspiration_2022-01-14T12_30_10-0700.lst'
            if self.target_clients and core_name not in self.target_clients: 
                continue
            else:
                pass
            with open(ddl_output_file_path, 'w', newline='') as sqlfile:
                with open(ddl_input_file_path, "r") as file:
                    tablecount=0
                    table_name = ""
                    useSchema = "  USE SCHEMA " + core_name.upper() + ";\n"
                    write_ddl_row(SnowflakeDialect.useDatabase.format(database=self.databaseName), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list) 
                    write_ddl_row(useSchema, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                    next_line_hold = None
                    table_key = ""
                    columns = ""
                    constraint = ""
                    while True:
                            next_line = file.readline().replace('"','')
                            if not next_line: 
                                break
                            if 'NOT NULL ENABLE' in next_line:
                                next_line = next_line.replace(" NOT NULL ENABLE","")
                            if 'NOVALIDATE' in next_line:
                                next_line = next_line.replace(" NOVALIDATE","")
                            if '$' in next_line and 'CREATE TABLE' not in next_line:
                                next_line = next_line.replace("$","")
                            if 'CREATE UNIQUE INDEX' in next_line:
                                next_line_hold = True
                                continue  
                            if 'ONTROL ON' in next_line:
                                next_line_hold = True
                                continue  
                            if 'UNIQUE' in next_line and 'ADD CONSTRAINT' in next_line:
                                next_line_hold = True
                                continue  
                            if 'ADD UNIQUE' in next_line:
                                next_line_hold = True
                                continue  
                            if 'CHECK' in next_line and 'ENABLE' in next_line:
                                next_line_hold = True
                                continue  
                            if 'CREATE INDEX' in next_line:
                                next_line_hold = True
                                continue  
                            if 'USING INDEX' in next_line:
                                next_line_hold = True
                                continue
                            if 'ADD SUPPLEMENTAL' in next_line: # Exclusive to Galileo Schema
                                next_line_hold = True
                                continue
                            if 'ALTER INDEX' in next_line: # Exclusive to Galileo Schema
                                next_line_hold = True
                                continue
                            if next_line_hold:
                                next_line_hold = None
                                continue
                            if 'CONSTRAINT' in next_line and '#' in next_line: # Syntax issue example at line 12364 from utils/snowflake/schema_change/input/data_sources/cv/2022-09-19/tab_ddl_aue1cv02_chm4_2022-09-19T10_28_48-0700.lst
                                if '(' in next_line[0:5]:
                                    next_line = re.sub("CONSTRAINT.*$",repl=',',string=next_line).replace('(','',1)
                                    write_ddl_row(next_line, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    continue
                                else:
                                    write_ddl_row(re.sub("CONSTRAINT.*$",repl=',',string=next_line), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    continue
                            if 'PRIMARY KEY' in next_line:
                                extract_columns = next_line.split(' ')
                                table_name = extract_columns[2].replace('"','').split(".")[1].strip().replace('$','')
                                table_key = (self.databaseName +'.'+core_name +'.' + table_name).upper()
                                # extract columns
                                constraint = 'PK_'+ core_name + '_' + table_name
                                start_index = next_line.index('(')
                                columns = next_line[start_index:]
                                primary_key_columns_alter_mode = columns.replace('"','').strip()
                                primary_key_columns_sproc_mode = convert_pk_constraint_string_to_list(columns)
                                #print(table_key+ '' + constraint + columns)
                                if sproc_mode:
                                    sproc_primary_key_statement = SnowflakeDialect.primary_key_sproc_template.format(full_qualified_table_name=table_key, constraint_name=constraint, column_names_array=primary_key_columns_sproc_mode)
                                    write_ddl_row('\n--<alter statement separator>', sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    write_ddl_row(sproc_primary_key_statement, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                else:
                                    alter_primary_key_statement = SnowflakeDialect.alter_primary_key.format(full_qualified_table_name=table_key, constraint_name=constraint, column_names_array=primary_key_columns_alter_mode)
                                    write_ddl_row('\n--<alter statement separator>', sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    write_ddl_row(alter_primary_key_statement, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue
                            if "PARTITION" in next_line and table_name != "":     # skip the like with partition word           
                                if "PARTITION BY RANGE" in next_line and ';' in next_line:
                                    write_ddl_row(';', sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                if "PARTITION BY RANGE" in next_line and 'INTERVAL' in next_line:
                                    write_ddl_row(';', sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue
                            if "PARALLEL 16" in next_line:          
                                if "PARALLEL 16" in next_line and ';' in next_line:
                                    write_ddl_row(';', sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue
                            if "'ultra', 'uscurdw', 'usdw'" in next_line:          
                                continue
                            if "LOB (" in next_line:
                                table_name=""
                                write_ddl_row(';\n\n', sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue
                            if "ORGANIZATION EXTERNAL" in next_line:
                                table_name= ""
                                write_ddl_row(';\n\n', sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue
                            if "ENABLE STORAGE" in next_line:         
                                table_name=""
                                write_ddl_row(';\n\n', sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue
                            if "ENABLE ROW MOVEMENT" in next_line:     # skip the like with ENABLE ROW MOVEMENT word           
                                if "ENABLE ROW MOVEMENT" in next_line and ';' in next_line:
                                    write_ddl_row(');', sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue
                            
                            if ")   NO INMEMORY ;" in next_line:     # remove NO INMEMORY       
                                if ")" in next_line:
                                    replaced_line = next_line.replace(")   NO INMEMORY ;",");").strip() 
                                    write_ddl_row(replaced_line, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                else:
                                    replaced_line = next_line.replace("NO INMEMORY","").strip()
                                    write_ddl_row(replaced_line, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue
                            
                            if "MONITORING" in next_line:     
                                #write_ddl_row(next_line.replace("MONITORING","").strip(), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue

                            if "CREATE GLOBAL TEMPORARY" in next_line:  # skip the temp table  
                                table_name=""
                                continue   

                            if "DR$" in next_line:  # skip the monitoring table 
                                table_name=""
                                #print("----skipping --",next_line)
                                continue   

                            if " BLOB" in next_line and table_name != "":  # replace BLOB -> BINARY
                                replaced_next_line = next_line.replace(" BLOB"," BINARY")
                                write_ddl_row(replaced_next_line, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue 

                            if " LONG" in next_line and table_name != "":  # replace LONG -> STRING
                                replaced_next_line = next_line.replace(" LONG"," STRING")
                                write_ddl_row(replaced_next_line, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue   

                            if " NUMBER(*,0)" in next_line and table_name != "":  # replace (*,0) -> ''
                                
                                write_ddl_row(next_line.replace(r'(*,0)','').replace('(	',''), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue   

                            if "\tSTART " in next_line and table_name != "":  # START -> _START_''
                                write_ddl_row(next_line.replace('\tSTART','\t_START_'), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue   

                            if " CLOB" in next_line and table_name != "":  # replace CLOB -> VARCHAR
                                replaced_next_line = next_line.replace(" CLOB"," VARCHAR")
                                write_ddl_row(replaced_next_line, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue   

                            if " NCLOB" in next_line and table_name != "":  # replace CLOB -> VARCHAR
                                replaced_next_line = next_line.replace(" NCLOB"," VARCHAR")
                                write_ddl_row(replaced_next_line, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue   

                            if " RAW" in next_line and table_name != "":  # replace RAW -> VARIANT
                                #replaced_next_line = next_line.replace(" RAW"," VARIANT")
                                if '$' in next_line:
                                    write_ddl_row(re.sub("RAW.*$",repl='VARIANT,',string=next_line).replace('$',''), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                else:
                                    write_ddl_row(re.sub("RAW.*$",repl='VARIANT,',string=next_line), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                #write_ddl_row(replaced_next_line, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue   
                            
                            if ' ENCRYPT USING' in next_line and table_name != "":
                                i += 1
                                #print("Columns with ENCRYPT USING: ", i)
                                if next_line.endswith(',',0 ,-1):
                                    #print("removing...",re.sub("ENCRYPT USING.*$",repl=',',string=next_line))
                                    write_ddl_row(re.sub("ENCRYPT USING.*$",repl=',',string=next_line), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                elif next_line.endswith('\n') and next_line.endswith(',',0,-1) == False:
                                    #print("removing...",re.sub("ENCRYPT USING.*$",repl='',string=next_line))
                                    write_ddl_row(re.sub("ENCRYPT USING.*$",repl='',string=next_line), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue

                            if " DEFAULT" in next_line and table_name != "": # remove DEFAULTS

                                if next_line.endswith(',',0 ,-1):
                                    if '(' in next_line[0:5]:
                                        write_ddl_row(DATE_TO_TIMESTAMP_NTZ(re.sub("DEFAULT.*$",repl=',',string=next_line).replace('(','',1)), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    else:
                                        write_ddl_row(DATE_TO_TIMESTAMP_NTZ(re.sub("DEFAULT.*$",repl=',',string=next_line)), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                elif next_line.endswith('\n') and next_line.endswith(',',0,-1) == False:
                                    write_ddl_row(DATE_TO_TIMESTAMP_NTZ(re.sub("DEFAULT.*$",repl='',string=next_line)), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue
                            
                            if "$" in next_line and table_name != "":
                                # if 'SNAPTIME$$' in next_line:
                                #     pass
                                if 'DATETIME' in next_line:
                                    pass
                                elif 'CREATE TABLE' in next_line:
                                    pass     # remove $ sign
                                elif '(' in next_line:
                                    pass
                                    # handling cases when line contains '$' and 'DATE'
                                else:
                                    write_ddl_row(next_line.replace("$",""), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)

                            if 'WITH TIME ZONE' in next_line and table_name != "":  # replace WITH TIMEZONE -> TIMESTAMP_TZ
                                replaced_next_line = next_line.replace('WITH TIME ZONE','').replace('TIMESTAMP','TIMESTAMP_TZ')
                                write_ddl_row(replaced_next_line, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue
                            if " DATE" in next_line and table_name != "":  # replace DATE -> DATETIME
                                if '$' in next_line:
                                    replaced_next_line = next_line.replace('$','')
                                elif '(' in next_line[0:5]:
                                    replaced_next_line = next_line.replace(" DATE"," TIMESTAMP_NTZ").replace('(','')
                                else:
                                    replaced_next_line = next_line.replace(" DATE"," TIMESTAMP_NTZ")
                                write_ddl_row(replaced_next_line, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue   
                            
                            if " GENERATED ALWAYS AS" in next_line and table_name != "": # remove VIRTUAL Columns
                                if next_line.endswith(',') or next_line.endswith(',',0,-1):
                                    write_ddl_row(re.sub(" GENERATED ALWAYS AS.*$",repl=',',string=next_line), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                elif next_line.endswith(',') == False or next_line.endswith(',',0,-1) == False:
                                    write_ddl_row(re.sub(" GENERATED ALWAYS AS.*$",repl='',string=next_line), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)

                                continue

                            if "UROWID" in next_line and table_name != "": # replace UROWID(400) -> VARCHAR
                                write_ddl_row(re.sub("UROWID.*$",repl='VARCHAR,',string=next_line), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue

                            if "SYS.XMLTYPE" in next_line and table_name != "": # replace SYS.XMLTYPE -> VARIANT
                                replaced_next_line = next_line.replace("SYS.XMLTYPE","VARIANT")
                                write_ddl_row(replaced_next_line, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                continue
                            
                            if OracleDialect.end_of_ddl_file_date[0] in next_line:
                                continue
                            elif OracleDialect.end_of_ddl_file_date[1] in next_line:
                                continue
                            
                            if OracleDialect.end_of_file_bad_sql in next_line:
                                continue

                            if  "CREATE TABLE"  in  next_line: # get tablename
                                table_name = next_line.replace('"','').split(".")[1].strip().replace('$','')
                                if tables_filter is None:
                                    table_created_at = CURRENT_US_PACIFIC_DATETIME
                                    table_key = (core_name +'.' + table_name).upper()
                                    #print(table_name)
                                    tablecount+=1  
                                    write_ddl_row('  --<statement separator>\n', sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    write_ddl_row(next_line.replace("CREATE TABLE","CREATE TABLE IF NOT EXISTS").replace('(','').replace('$',''), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    write_ddl_row(SnowflakeDialect.tableComment.format(self.data_source,self.data_source_ddl_extracted_at,table_created_at), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    write_ddl_row("\n", sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    if include_clustering_keys and table_key in Paths.readPartitionTables():
                                        cluster_column = Paths.readPartitionTables()[table_key]
                                        #.replace('<>')
                                        write_ddl_row(SnowflakeDialect.tableClusterBody.format(cluster_key_column=cluster_column), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    
                                    write_ddl_row(SnowflakeDialect.metadataColumns, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list) 
                                    continue
                                elif table_name in tables_filter and tables_filter_exclude == True:
                                    table_name = ""
                                    continue
                                elif table_name not in tables_filter and tables_filter_exclude == True:
                                    table_created_at = CURRENT_US_PACIFIC_DATETIME
                                    table_key = (core_name +'.' + table_name).upper()
                                    #print(table_name)
                                    tablecount+=1  
                                    write_ddl_row(next_line.replace("CREATE TABLE","CREATE TABLE IF NOT EXISTS").replace('(',''), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    write_ddl_row(SnowflakeDialect.tableComment.format(self.data_source,self.data_source_ddl_extracted_at,table_created_at), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    write_ddl_row("\n", sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    if include_clustering_keys and table_key in Paths.readPartitionTables():
                                        cluster_column = Paths.readPartitionTables()[table_key]
                                        #.replace('<>')
                                        write_ddl_row(SnowflakeDialect.tableClusterBody.format(cluster_key_column=cluster_column), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    
                                    write_ddl_row(SnowflakeDialect.metadataColumns), sqlfile 
                                    continue
                                elif table_name in tables_filter  and tables_filter_exclude == False:
                                    table_created_at = CURRENT_US_PACIFIC_DATETIME
                                    table_key = (core_name +'.' + table_name).upper()
                                    #print(table_name)
                                    tablecount+=1  
                                    write_ddl_row(next_line.replace("CREATE TABLE","CREATE TABLE IF NOT EXISTS").replace('(',''), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    write_ddl_row(SnowflakeDialect.tableComment.format(self.data_source,self.data_source_ddl_extracted_at,table_created_at), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    write_ddl_row("\n", sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    if include_clustering_keys and table_key in Paths.readPartitionTables():
                                        cluster_column = Paths.readPartitionTables()[table_key]
                                        #.replace('<>')
                                        write_ddl_row(SnowflakeDialect.tableClusterBody.format(cluster_key_column=cluster_column), sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                                    
                                    write_ddl_row(SnowflakeDialect.metadataColumns), sqlfile 
                                    continue
                                else:
                                    table_name = ""
                                    continue

                            if len(table_name) > 0: 
                                if next_line.strip().startswith('('):
                                    next_line=next_line.replace('(','',1)
                                if "   (" in next_line:
                                    next_line=next_line.replace('(','')
                                write_ddl_row(next_line, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)                           
                    
                    file_version_number = '.'.join(re.sub("__.*$",repl='',string=ddl_output_file_path.split(r'/')[2]).split('.')[1:3])
                    #self.versioningList.append(file_version_number)
                    total_tables += tablecount

        if clients_count ==0:
            clients_count = 1
        ############## Logging 
        utilLogger.info(msg=f"Tables per Client:  {total_tables/clients_count:,.2f}")
        utilLogger.info(msg=f"Total Clients: {clients_count:,.2f}")
        utilLogger.info(msg=f"Total Tables: {total_tables:,.2f}")

    def generate_first_dump_pipes(self, file_format_name: str,
                                  s3_stage_is_prod: bool = True):
        #version = self.__version - (len(self.clients) * 2) - 1
        """Writes Snowflake SQL DDL Files for each client - Snowpipe Creation."""
        file_format_schema = 'SHARED.PUBLIC.'
        file_format = f"{file_format_schema}{file_format_name}"
        output_data_source_path = SnowflakeDialectManager.outputDataSourcePathDict[self.data_source]
        s3_stage_name = ""
        if s3_stage_is_prod:
            s3_stage_name = SnowflakeDialect.stageNames["PROD"]
        elif not s3_stage_is_prod:
            s3_stage_name = SnowflakeDialect.stageNames["PERF"]

        if not s3_stage_is_prod:
            utilLogger.info(f"could not figure out s3_stage_name.")
            exit(1)
        
        utilLogger.info(f"Creating Snowpipes for {self.data_source} data source.")
        utilLogger.info(f"Utilizing {s3_stage_name} S3 Bucket Stage.")
        
        groups = sorted(os.listdir(output_data_source_path))

        for group in groups:
            if group.startswith('.DS'):
                    break
            group_path = os.path.join(output_data_source_path, group)
            utilLogger.info(f"group_path")
            clients = sorted(os.listdir(group_path))
            for client in clients:
                schema_name = client.upper() 
                if self.target_clients and client not in self.target_clients: 
                    continue
                client_path = os.path.join(group_path,client)
                
                for table_file in os.listdir(client_path):
                    table_pipe = ""
                    tables_path = os.path.join(client_path,table_file)
                    fname = f"{schema_name}_{CURRENT_US_PACIFIC_DATE}_b_create_pipe.sql"
                    #version += self.__incrementBy
                    pipe_path = os.path.join(client_path,fname)
                    
                    with open(pipe_path,'w') as pipefile:
                        with open(tables_path,'r') as tablefile:
                            pipe_count=0
                            pipe_name = ""
                            useSchema = "USE SCHEMA "+client.upper()+";\n\n"
                            pipefile.write(SnowflakeDialect.useDatabase.format(database=self.databaseName)) # use DB
                            pipefile.write(useSchema) # use DB 

                            while True:
                                next_line = tablefile.readline().replace('"','')
                                if not next_line:
                                    break
                                
                                if "CREATE TABLE" in next_line:  # get tablename
                                    table_name = next_line.split(".")[1].strip()
                                    table_fqn = (self.databaseName + '.' + schema_name + '.' + table_name).upper()
                                    pipe_name = table_fqn + '_PIPE'
                                    table_pipe = SnowflakeDialect.pipeBody.format(db=self.databaseName
                                                    ,schema=schema_name
                                                    ,table_name=table_name
                                                    ,pipe_fqn=pipe_name
                                                    ,s3_stage=s3_stage_name
                                                    ,fileFormatFqn=file_format) +'\n\n'

                                    pipefile.write(table_pipe)
                                    continue

    def generate_first_dump_operational_pipes(self, file_format_name: str,
                                                    s3_stage_is_prod: bool = True):
        #file_format_name = "ORACLE_EXPORT_CSV_FILE_FORMAT"
        # version = self.__version - (len(self.clients) * 2) - 1
        """Writes Snowflake SQL DDL Files for each client - Snowpipe Creation."""
        cursor, conn = self.initialize_snowflake_connection()
        snowflake_query_executor = SnowflakeExecutor(cursor, conn)
        file_format_schema = 'SHARED.PUBLIC.'
        file_format = f"{file_format_schema}{file_format_name}"
        output_data_source_path = SnowflakeDialectManager.outputDataSourcePathDict[self.data_source]
        s3_stage_name = ""

        if s3_stage_is_prod:
            s3_stage_name = SnowflakeDialect.stageNames["PROD_OPERATIONAL"]
        elif not s3_stage_is_prod:
            s3_stage_name = SnowflakeDialect.stageNames["PERF_OPERATIONAL"]

        #if not s3_stage_is_prod:
        #    utilLogger.info(f"could not figure out s3_stage_name.")
        #    exit(1)

        utilLogger.info(f"Creating Snowpipes for {self.data_source} data source.")
        utilLogger.info(f"Utilizing {s3_stage_name} S3 Bucket Stage.")

        groups = sorted(os.listdir(output_data_source_path))

        for group in groups:
            if group.startswith('.DS'):
                break
            group_path = os.path.join(output_data_source_path, group)
            utilLogger.info(f"{group_path}")
            clients = sorted(os.listdir(group_path))
            for client in clients:
                schema_name = client.upper()
                if self.target_clients and client not in self.target_clients:
                    continue
                client_path = os.path.join(group_path, client)

                for table_file in os.listdir(client_path):
                    if "pipe" in table_file:
                        continue
                    table_pipe = ""
                    operational_table_pipe = ""
                    tables_path = os.path.join(client_path, table_file)
                    fname = f"{schema_name}_{CURRENT_US_PACIFIC_DATE}_c_create_operational_pipe.sql"
                    # version += self.__incrementBy
                    pipe_path = os.path.join(client_path, fname)

                    with open(pipe_path, 'w') as pipefile:
                        with open(tables_path, 'r') as tablefile:
                            pipe_count = 0
                            pipe_name = ""
                            useSchema = "USE SCHEMA " + client.upper() + ";\n\n"
                            pipefile.write(SnowflakeDialect.useDatabase.format(database=self.databaseName))  # use DB
                            pipefile.write(useSchema)  # use DB
                            counter = 0
                            while True:
                                next_line = tablefile.readline().replace('"', '')
                                if not next_line:
                                    break

                                if "CREATE TABLE" in next_line:  # get tablename
                                    counter = 0
                                    table_name = next_line.split(".")[1].strip()

                                if ") ;" in next_line:
                                    #counter=snowflake_query_executor.number_of_columns(database= self.databaseName,schema=schema_name ,table=table_name)
                                    operational_table_pipe = SnowflakeDialectManager.create_operational_pipe(
                                        self.databaseName,
                                        schema_name,
                                        table_name,
                                        s3_stage_name,
                                        counter-4,
                                        file_format
                                    )

                                    if operational_table_pipe:
                                        pipefile.write(operational_table_pipe+'\n\n')
                                    continue
                                counter+=1
                            tablefile.close()
                        pipefile.close()

    @staticmethod
    def generate_secure_data_db_tables(tokenized_schema_tables_dict:dict,env:str):
        prefix_path='utils/snowflake/schema_change/output/secure_db/'
        
        if env=="PRD":
            prefix_path_with_env = prefix_path + 'production'
        elif env=="ST":
            prefix_path_with_env = prefix_path + 'system_test'
        
        ## Creating Directories in case they don't exist yet
        if os.path.exists(prefix_path)==False:
            os.mkdir(prefix_path)
        if os.path.exists(prefix_path_with_env)==False:
            os.mkdir(prefix_path_with_env)
            
        for schema_name in tokenized_schema_tables_dict:
            fname = f"{env.lower()}_SECURE_DATA_DB_{schema_name}_TABLES_{CURRENT_US_PACIFIC_DATE}.sql"
            with open(f"{prefix_path_with_env}/{fname}",'w') as securedata_ddl_file:
                #securedata_ddl_file.write(f"--Total count of tables = {table_count}\n")
                for table_name in tokenized_schema_tables_dict[schema_name]:
                    table_ddl = SnowflakeDialect.secure_db_tables_template.format(schema_name=schema_name, table_name=table_name)
                    securedata_ddl_file.write(table_ddl)
                    securedata_ddl_file.write("\n\n")
        return prefix_path_with_env
            
                
    @staticmethod
    def create_operational_pipe(db_name,
                                schema_name,
                                table_name,
                                s3_stage_name,
                                column_count,
                                file_format
                                ):
        table_fqn = (db_name + '.' + schema_name + '.' + table_name).upper()
        # column_count = snowflake_query_executor.get_column_count(self.databaseName, table_name, schema_name)
        operational_pipe_name = table_fqn + '_OPERATIONAL_PIPE'

        utilLogger.info(f"Creating Operational Snowpipe for {db_name} data source and db {operational_pipe_name}.")
        operational_pipe_name_statement = ""

        # create selective statement
        select_statement = ""
        for counter in range(0, column_count):
            if counter == 5:
                select_statement += "CURRENT_TIMESTAMP(),"
                select_statement += f"A.${counter+1}"
            else:
                select_statement += f"A.${counter+1}"

            if counter != column_count-1:
                select_statement += ","

        operational_pipe_name_statement = SnowflakeDialect.operationalPipeBody.format(
            pipe_fqn=operational_pipe_name,
            db_name=db_name,
            schema_name=schema_name,
            table_name=table_name,
            column_list=select_statement,
            stage=s3_stage_name,
            file_format=file_format
        )

        return operational_pipe_name_statement

    def get_columns_ordinal_position(self,client_name):
        for output_file_path in self.first_dump_ddl_file_paths:
            if 'create_pipe.sql' in output_file_path:
                continue
            client = output_file_path.split('/')[4].upper() #schema_change_utilsf/{first_dump_ddl_dir_path}/cv/A/amsysis
            if client != client_name:
                continue
            z = 0
            with open(output_file_path,'r') as file:
                print(client)
                while True:
                    next_line = file.readline().replace('"','').rstrip().strip()
                    # if not next_line: 
                    #     break
                    table = ''
                    counter=-1
                    if 'CREATE TABLE' in next_line:
                        table = next_line.split(' ')[5].split('.')[1]
                        self.columns_ordinal_position[table] = {}
                        if table.startswith('Z'):
                            z =+ 1
                    if '$$break$$' in next_line:
                        break

                    while True and table != '':
                        print(next_line)
                        next_line = file.readline().replace('"','').strip()
                        if  next_line.__contains__(') ;') | next_line.__contains__(');'):
                            break
                        if    next_line.__contains__('COMMENT =') == False |\
                            next_line.__contains__('CLUSTER BY') == False|\
                            next_line.__contains__('	(') == False|\
                            next_line.__contains__('DATA_INGESTION_TIME') == False|\
                            next_line.__contains__('SHARED.PUBLIC."GET_TS_PART"') == False:
                            if counter == -1:
                                counter += 1
                                continue
                            column_name = next_line.split(' ')[0]
                            self.columns_ordinal_position[table][counter] = column_name
                            counter += 1
                            continue
                
    def generate_copy_history_query(self, target_client,db_name,past_hours):
        """Copy History Query generation to check healthy
        status of ingestion for hundreds of tables."""
        self.get_clients_tables()
        tables = self.clients_tables[target_client.upper()]['tables_list']
        tables = [table for table in tables if '$' not in table]
        outputFile = f'{"utils/snowflake/schema_change/output/copy_history_query"}/{self.data_source}_{target_client}_{CURRENT_US_PACIFIC_DATE}.sql'
        with open(outputFile, 'w') as file:
            file.write('WITH\n')
            for t in tables:
                query = SnowflakeDialect.copy_history_query_body.format(table_name=t,client_name=target_client,db_name=db_name, past_hours=past_hours)
                file.write(query)
                file.write('\n')
            for t in tables:
                statment_body = '\nSELECT * from {table_name} \nUNION ALL'.format(table_name=t)
                file.write(statment_body)
            file.write(';')

    def apply_masking_policy(self, client, pii_columns_rows, maskingPolicyFQN="SHARED.PUBLIC.SHOW_HASH_ONLY_POLICY"):        
        """Writes ALTER TABLE statements for each client
        at first_dump_ddl dir to set Masking Policy for PII Columns.
        """
        self.get_clients_tables()
        alter_statements_list = []
        #for client in self.clients_tables:
        for row_dict in pii_columns_rows:
            if str(row_dict['masking_rule_id']) != 'None':
                table_name = row_dict['table_name']
                column_name = row_dict['column_name']
                column_datatype = row_dict['column_datatype']
                masking_rule_id = int(row_dict['masking_rule_id'])
                maskingPolicyFQN = SnowflakeDialect.masking_policies.get(masking_rule_id)
                if (masking_rule_id in [2] or 'VALUE' in column_name):
                    continue
                if table_name not in self.clients_tables[client] or client not in self.clients:
                    continue                
                alter_statement = SnowflakeDialect.masking_policy_alter_statement(  db=self.databaseName,
                                                                                    schema=client,
                                                                                    table=table_name,
                                                                                    column=column_name,
                                                                                    masking_policy_fqn=maskingPolicyFQN,
                                                                                    dtype=column_datatype
                                                                                )
                alter_statements_list.append((alter_statement,))
        
        fileName = self.output_data_source_file_paths[client].split('/')[-1]
        date = self.data_source_ddl_extracted_at + '/'
        fpath = self.output_data_source_file_paths[client].replace('data_sources','first_dump_ddls').replace(fileName,client[0]).replace(date,'')
        fpath = f"{fpath}/{client}/R__{client}_{CURRENT_US_PACIFIC_DATE}_apply_masking_policy.sql"
        with open(fpath,'a') as file:
            print(f'Writing {len(alter_statements_list)} statements for core {client} at path {fpath}')
            writer = csv.writer(file,quoting=csv.QUOTE_NONE,doublequote=False,escapechar=' ')
            writer.writerows(alter_statements_list)
        # empty list of statements for next client
        alter_statements_list = []
        return alter_statements_list

    def alter_add_column(self, table:str, newColumnName:str, dataType:str, clientFilterList = "*"):

        r_file = 'R__' + now + self.data_source + "_output.sql"
        fpath = os.path.join("utils/snowflake/schema_change/output/alter_table_statements/add_column", r_file)

        print("-> Adding New Column...")
        print("\t- DB Name: " , self.databaseName.upper())
        print("\t- New Column Name: " ,newColumnName.upper())
        print("\t- New Data Type Name: " ,dataType.upper())
        print("\t- Output Path: ", fpath)
        # TO DO
        clients = self.clients
        print("List of Client(s) with New Column: ",', '.join(clients))

        with open(fpath,'w') as rfile:
            for client in clients:
                # get Client/Core/Schema name
                schema = client.upper()
                statementsList = []
                alter_statement = ['ALTER TABLE {db}.{sch}.{tbl} ADD COLUMN {col_name} {dtype};\n'.format(  db = self.databaseName,
                                                                                                            sch = schema,
                                                                                                            tbl = table,
                                                                                                            col_name=newColumnName,
                                                                                                            dtype = dataType
                                                                                                            )]

                statementsList.append(alter_statement)
                rfile.write(''.join(alter_statement))

    def alter_column_dtype(self, table:str, columnName:str, dataType:str, clientFilterList = "*"):
        r_file = 'R__' + now + self.data_source + "_"+ "alter_column_length"
        fpath = os.path.join("utils/snowflake/schema_change/output/alter_table_statements/column_length", r_file)
        
        print("-> ALTER Column Length...")
        print("\t- DB Name: " , self.databaseName.upper())
        print("\t- Column Name: " , columnName.upper())
        print("\t- New Data Type Name: " , dataType.upper())
        print("\t- Output Path: ", fpath)
        
        clients = self.clients

        with open(fpath,'w') as rfile:
            for client in clients:
                # get Client/Core/Schema name
                schema = client
                statementsList = []
                alter_statement = ['ALTER TABLE {db}.{sch}.{tbl} MODIFY COLUMN {col_name} {dtype};\n'.format(  db = self.databaseName.upper(),
                                                                                                            sch = schema.upper(),
                                                                                                            tbl = table.upper(),
                                                                                                            col_name=columnName.upper(),
                                                                                                            dtype = dataType.upper()
                                                                                                            )]
                statementsList.append(alter_statement)
                rfile.write(''.join(alter_statement))
    
    def generate_primary_key_alter_statemets(self, sproc_mode=True):
        for ddl_input_file_path, ddl_output_file_path  in zip(self.input_data_source_file_paths,self.first_dump_ddl_file_paths):
            core_name = ddl_input_file_path.split('/')[7].split('_')[3].upper() #'utils/snowflake/schema_change/input/data_sources/oracle_oltp/2022-01-14/tab_ddl_x1prc10p_aspiration_2022-01-14T12_30_10-0700.lst'
            r_file = core_name + '_' + str(datetime.today().date()) + '_' + "set_primary_key.sql"
            fpath = os.path.join("utils/snowflake/schema_change/output/primary_key/", r_file)
            if self.target_clients and core_name not in self.target_clients:
                continue
            else:
                pass
            with open(fpath, 'w', newline='') as sqlfile:
                with open(ddl_input_file_path, "r") as file:
                    tablecount=0
                   
                    useSchema = "  USE SCHEMA " + core_name.upper() + ";\n"
                    write_ddl_row(SnowflakeDialect.useDatabase.format(database=self.databaseName)), sqlfile 
                    write_ddl_row(useSchema, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                    bad_syntax_filter = None
                    while True:
                        table_name = ""
                        table_key = ""
                        columns = ""
                        constraint = ""
                        next_line = file.readline()
                        if not next_line:
                            break
                        if 'PRIMARY KEY' in next_line:
                            extract_columns = next_line.split(' ')
                            table_name = extract_columns[2].replace('"','').split(".")[1].strip().replace('$','')
                            table_key = (self.databaseName + '.' +core_name +'.' + table_name).upper()
                            # extract columns
                            constraint = 'PK_'+ core_name + '_' + table_name
                            start_index = next_line.index('(')
                            columns = next_line[start_index:]
                            primary_key_columns_alter_mode = columns.replace('"','').strip()
                            primary_key_columns_sproc_mode = convert_pk_constraint_string_to_list(columns)
                            print(table_key+ '' + constraint + columns)
                            if sproc_mode:
                                sproc_primary_key_statement = SnowflakeDialect.primary_key_sproc_template.format(full_qualified_table_name=table_key, constraint_name=constraint, column_names_array=primary_key_columns_sproc_mode)
                                write_ddl_row(sproc_primary_key_statement, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
                            else:
                                alter_primary_key_statement = SnowflakeDialect.alter_primary_key.format(full_qualified_table_name=table_key, constraint_name=constraint, column_names_array=primary_key_columns_alter_mode)
                                write_ddl_row(alter_primary_key_statement, sqlfile,table_name=table_name, tokenized_keys=token_columns_key_list)
        print(f"Output at: {fpath}")

    def append_on_oracle_input_schema(self, sql_text:str):
        """
        sql_text: text to be appended
        self.data_source: directory with files to be appended
        """ 
        now = CURRENT_US_PACIFIC_DATETIME
        for file in self.input_data_source_file_paths:
            fpath = os.path.join(self.__get_input_ddl_path(self.data_source), file)
            core_name = file.split('/')[4].split('_')[3] #'utils/snowflake/schema_change/input/data_sources/oracle_oltp/2022-01-14/tab_ddl_x1prc10p_aspiration_2022-01-14T12_30_10-0700.lst'
            client_sql_text  = sql_text.replace("CREATE TABLE ","CREATE TABLE "+ core_name + '.')
            
            with open(fpath,'a') as append_file:
                append_file.write('\n\n')
                append_file.write('-- New SQL DDL appended at: ' + now)
                append_file.write('\n\n')
                append_file.write(client_sql_text)
