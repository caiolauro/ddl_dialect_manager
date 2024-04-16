# Refs:
# https://pypi.org/project/simple-ddl-parser/
# https://pypi.org/project/deepdiff/
# https://www.adamsmith.haus/python/answers/how-to-get-the-difference-between-two-list-in-python

from simple_ddl_parser import parse_from_file, DDLParser
import sys
import os
import pytz
tz = pytz.timezone('US/Pacific')
from datetime import datetime, date
from utils.snowflake.connector.snowflake_connector import initialize_snowflake_connector_using_adp_python_user as initalize_snowflake
import json
#import pandas as pd

CREATED_AT = datetime.now(tz).strftime('%Y-%m-%dT%H-%M-%S')

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

def get_ddl_data_source_dates(data_source):
    """
    ## What it does
    Retrieves a list of existing subdirectories inside 
    the given data_source directory.
    ## Example
    - path to data_source directory: utils/snowflake/schema_change/input/data_sources/aws_rds
    - output: [2022-05-10, ..., 2022-06-21]
    """
    output_ds_path = f'utils/snowflake/schema_change/output/data_sources/{data_source.lower()}'
    return [os.path.join(output_ds_path, date) for date in sorted(os.listdir(output_ds_path))]

def get_ddl_directories_from_data_source(data_source):
    input_ds_path = f'utils/snowflake/schema_change/input/data_sources/{data_source.lower()}'
    output_ds_path = f'utils/snowflake/schema_change/output/data_sources/{data_source.lower()}'

    list_of_input_dates = os.listdir(input_ds_path)
    list_of_output_dates = os.listdir(output_ds_path)
    ilen = len(list_of_input_dates) - 2
    olen = len(list_of_output_dates) - 2
    most_recent_date_from_input = max(  [ date(int(ds_date.split('-')[0]), int(ds_date.split('-')[1]), int(ds_date.split('-')[2])) for ds_date in list_of_input_dates ])
    most_recent_date_from_output = max(  [ date(int(ds_date.split('-')[0]), int(ds_date.split('-')[1]), int(ds_date.split('-')[2])) for ds_date in list_of_output_dates ])
    past_date_from_input = sorted([ date(int(ds_date.split('-')[0]), int(ds_date.split('-')[1]), int(ds_date.split('-')[2])) for ds_date in list_of_input_dates ])[ilen]
    past_date_from_output = sorted([ date(int(ds_date.split('-')[0]), int(ds_date.split('-')[1]), int(ds_date.split('-')[2])) for ds_date in list_of_output_dates ])[olen]
    
    path_to_most_recent_ddl_input_dir = f"{input_ds_path}/{most_recent_date_from_input}"
    path_to_most_recent_ddl_output_dir = f"{output_ds_path}/{most_recent_date_from_output}"
    path_to_past_ddl_from_input = f"{input_ds_path}/{past_date_from_input}"
    path_to_past_ddl_from_output = f"{output_ds_path}/{past_date_from_output}"
    outputDict = {
                  "past_input": path_to_past_ddl_from_input
                , "current_input":path_to_most_recent_ddl_input_dir 
                , "past_output": path_to_past_ddl_from_output
                , "current_output":path_to_most_recent_ddl_output_dir 
            }
    #print(outputDict)
    return outputDict

def parse_ddl_file(input_ddl_file_path):
    """
    ## What it does
    Retrieves JSON output from DDL file.
    ## Note
    The parser is not able to read 
    NUMBER(*,0)
    VARCHAR(<length> CHAR)
    """
    return parse_from_file(input_ddl_file_path)

def get_ddl_tables(ddl_dict):
    """
    ## What it does
    Retrieves list of tables contained
    in DDL JSON object.
    """
    return sorted([table_name for table_name in ddl_dict])

def get_tables_from_current_change_window(ddl_tables_from_past, ddl_tables_from_current_change_window ):
    """
    ## What it does
    Compares lists and retrieve tables 
    which are new.
    """
    ddl_added_tables_from_current_change_window  = []
    for table in ddl_tables_from_current_change_window :
        if table not in ddl_tables_from_past:
            ddl_added_tables_from_current_change_window.append(table)
    return ddl_added_tables_from_current_change_window 


def get_ddl_dictionary(input_ddl_file_path):
    """
    ## What it does
    Parses the input DDL file and outputs a dictionary
    containing information at column level for each table from the DDL.
    *Dictionary Keys: <schema_name>.<table_name> (e.g 'WALGREENS.VISA_AUTH')
    *Dictionary Values: list of dictionaries
        **Containig information about each column of the parent table.
    """
    try:
        raw_ddl_dict = parse_from_file(input_ddl_file_path)
    except Exception as e:
        print(f"\nERROR:\nWhen reading DDL File {input_ddl_file_path}, \nParser could not parse due bad syntax: {e}")
    ddl_output_dict = {}
    for table_object in raw_ddl_dict:
        try:
            schema = table_object['schema'].replace('"','')
            table = table_object['table_name'].replace('"','')
            table_name = schema +'.'+ table
            column_count = 0
            table_column_dictionary_list = []
            for table_column in table_object['columns']:
                table_column_dictionary = {}
                column_count += 1
                table_column_dictionary['schema'] = schema
                table_column_dictionary['table'] = table
                table_column_dictionary['column_index'] = str(column_count)
                table_column_dictionary['column_name'] = table_column['name'].replace('"','')
                table_column_dictionary['column_data_type'] = table_column['type']
                table_column_dictionary['column_length'] = f"({table_column['size']})" if table_column['size'] else ''
                table_column_dictionary_list.append(table_column_dictionary)
            ddl_output_dict[table_name] = table_column_dictionary_list
        except Exception as error:
            print(f"Error at table object:\n>> {table_object}\nERROR:\n>> {error}")

    return ddl_output_dict


def create_table(table_name,table_column_dictionaries, comparison_name, core):
    """
    ## What it does
    Appends CREATE TABLE statement for new 
    table that should be created.
    
    ***Args:
        table_name: table to be created
        list_of_column_tuples: tuple containing columns
        information (name, type, size)
    """
    base_directory = "utils/snowflake/schema_change/output/ddl_diffs/new_tables/"
    comparison_dates = comparison_name.split('__')[-1].replace('.sql','')
    comparison_name = comparison_name.replace('SCHEMACHANGE','TABLE_SCHEMACHANGE')
    path_to_comparison_group = f"{base_directory}/{comparison_dates}"
    path_to_core_group = f"{base_directory}/{comparison_dates}/{core[0]}"
    
    if os.path.exists(path_to_comparison_group):
        pass
    else:
        os.mkdir(path_to_comparison_group)
        
    if os.path.exists(path_to_core_group):
        pass
    else:
        os.mkdir(path_to_core_group)
        
    with open(f"{path_to_core_group}/{comparison_name}",'a') as outputFile:
        outputFile.write(f"""CREATE TABLE IF NOT EXISTS {table_name}\n(\n""")
        #tableComment = "\tCOMMENT = 'Environment: {0} ,DDL Extracted at: {1}, DDL Translation Generated at: {2}'"
        #outputFile.write(tableComment.format(environment))
        for column_dictionary in table_column_dictionaries:
            name = column_dictionary['column_name']
            type = column_dictionary['column_data_type']
            size = str(column_dictionary['column_length']).replace('((','(').replace('))',')')
            column_index = int(column_dictionary['column_index'])
            n_of_cols = len(table_column_dictionaries)
           
            if column_index != n_of_cols:
                 if size == 'None':
                    col_template = f"\t{name} {type},\n"
                 else:
                    col_template = f"\t{name} {type} {size},\n"
            else:
                if size == 'None':
                    col_template = f"\t{name} {type}\n"
                else:
                    col_template = f"\t{name} {type} {size}\n"
            
            outputFile.write(col_template)
        outputFile.write('\n);\n\n')


def create_operational_pipe(table_name,
                            table_column_dictionaries,
                            comparison_name,
                            core,
                            db_name,
                            is_changed_table=False,
                            file_format=None):
    """
    ## What it does
    Creates operational snowpipe for the table information provided.
    """
    print(comparison_name)
    base_directory = "utils/snowflake/schema_change/output/ddl_diffs/new_pipes/"
    if is_changed_table:
        base_directory = "utils/snowflake/schema_change/output/ddl_diffs/existing_pipes/"
    comparison_dates = comparison_name.split('__')[-1].replace('.sql', '')
    comparison_name = comparison_name.replace('SCHEMACHANGE', 'PIPE_SCHEMACHANGE')
    path_to_comparison_group = f"{base_directory}/{comparison_dates}"
    path_to_core_group = f"{base_directory}/{comparison_dates}/{core[0]}"

    if os.path.exists(path_to_comparison_group):
        pass
    else:
        os.mkdir(path_to_comparison_group)

    if os.path.exists(path_to_core_group):
        pass
    else:
        os.mkdir(path_to_core_group)

    with open(f"{path_to_core_group}/{comparison_name}", 'a') as outputFile:
        if db_name == 'ORACLE_RAW':
            s3_stage_name="@SHARED.PUBLIC.PRD_OPERATIONAL_EXPORTS_STAGE"
        elif db_name == 'S5PRC2C' or db_name == 'CV':
            s3_stage_name = "@SHARED.PUBLIC.PERF_OPERATIONAL_EXPORTS_STAGE"
        print(f"writing to {path_to_core_group}/{comparison_name} for {len(table_column_dictionaries)}")
        n_of_cols = len(table_column_dictionaries)
        operational_pipe = create_pipe(
            db_name=db_name,
            schema_name=core,
            table_name=table_name,
            s3_stage_name=s3_stage_name,
            column_count=n_of_cols-1,
            file_format=file_format
        )
        outputFile.write(operational_pipe)
        outputFile.write('\n);\n\n')


def create_pipe(db_name,
                schema_name,
                table_name,
                s3_stage_name,
                column_count,
                file_format
                ):
    print(table_name)
    table_fqn = (db_name + '.' + table_name).upper()
    # column_count = snowflake_query_executor.get_column_count(self.databaseName, table_name, schema_name)
    operational_pipe_name = table_fqn + '_OPERATIONAL_PIPE'

    operational_pipe_name_statement = ""

    # create selective statement
    select_statement = ""
    for counter in range(0, column_count):
        if counter == 5:
            select_statement += "CURRENT_TIMESTAMP()"
        else:
            select_statement += f"A.${counter+1}"

        if counter != column_count-1:
            select_statement += ","

    operational_pipe_name_statement = operationalPipeBody.format(
        pipe_fqn=operational_pipe_name,
        db_name=db_name,
        schema_name=schema_name,
        table_name=table_name,
        column_list=select_statement,
        stage=s3_stage_name,
        file_format=file_format
    )

    return operational_pipe_name_statement


def create_new_tables_from_current_change_window(
    ddl_dict_from_past_change_window, 
    ddl_dict_from_current_change_window, 
    comparison_name,
    core = None,
    db_name=None,
    file_format=None
    ):
    """
    ## What it does
    Gets list of new tables and invokes create_table function
    to generate DDL for all of them.
    """
    ddl_tables_from_past = get_ddl_tables(ddl_dict_from_past_change_window)
    ddl_tables_from_current_change_window  = get_ddl_tables(ddl_dict_from_current_change_window)

    ddl_added_tables_from_current_change_window  = get_tables_from_current_change_window(ddl_tables_from_past, ddl_tables_from_current_change_window )
    for table_from_current_change_window  in ddl_added_tables_from_current_change_window :
        create_table(
            table_from_current_change_window ,
            ddl_dict_from_current_change_window[table_from_current_change_window],
            comparison_name,
            core
            )

        create_operational_pipe(
            table_from_current_change_window,
            ddl_dict_from_current_change_window[table_from_current_change_window],
            comparison_name,
            core,
            db_name,
            False,
            file_format
        )


def get_schema_changes_between_ddls(ddl_dict_from_past_change_window, ddl_dict_from_current_change_window, core):
    """
    ## What it does
    Retrieves 2 tuples:

    - non_existent_tuples: information from new columns 
    contained in the ddl
    
    - data_type_modify_list: information about columns which
    had their data types modified.

    ***args:
    core ddl dictionary from past change window
    core ddl dictionary from current change window
    """
    ddl_tables_from_past = get_ddl_tables(ddl_dict_from_past_change_window)
    ddl_tables_from_current_change_window  = get_ddl_tables(ddl_dict_from_current_change_window)
    ddl_added_tables_from_current_change_window  = get_tables_from_current_change_window(ddl_tables_from_past, ddl_tables_from_current_change_window)
    
    filtered_ddl_tables_from_past = {k: v for k, v in ddl_dict_from_past_change_window.items() if k in ddl_tables_from_current_change_window } # get tables that exist both in past and new current
    filtered_ddl_tables_from_current_change_window  = {k: v for k, v in ddl_dict_from_current_change_window.items() if (k in ddl_tables_from_past) and (k not in ddl_added_tables_from_current_change_window )} # exclude tables which are new
    
    print(f"""Parsing Diffs for {core}
    past Change Window DDL containing {len(ddl_tables_from_past)} tables  
    Current Change Window DDL containing {len(ddl_tables_from_current_change_window)} tables  
    New tables detected in Current Change Window DDL: {len(ddl_added_tables_from_current_change_window )}""")

    non_existent_columns = []
    data_type_modify_list = []
    non_existent_columns_tables = []
    for table_from_past, table_from_current_change_window  in zip(filtered_ddl_tables_from_past,filtered_ddl_tables_from_current_change_window):
        if table_from_past != table_from_current_change_window :
            print(f"Table from Past DDL Dict {table_from_past} does not match Current DDL Dict Table {table_from_current_change_window }")
            sys.exit()



        # symetricaly iterate over list of dicts of each table
        for table_column_dictionary_from_past, table_column_dictionary_from_current_change_window  in zip(filtered_ddl_tables_from_past[table_from_past], filtered_ddl_tables_from_current_change_window[table_from_current_change_window]):
            
            if table_column_dictionary_from_past != table_column_dictionary_from_current_change_window:
                
                print(

                f"""Difference detected at column {table_from_past}.{table_column_dictionary_from_past['column_name']}.
                >>> {table_column_dictionary_from_past} ---> {table_column_dictionary_from_current_change_window }."""
                
                )
                data_type_modify_list.append(
                {
                    'column_dict_from_past':table_column_dictionary_from_past,
                    'column_dict_from_current_change_window':table_column_dictionary_from_current_change_window
                }
                
                )

        table_column_dictionary_from_current_change_window  = None

        dtype_dictionary_list_from_current_change_window  = [dict['column_dict_from_current_change_window'] for dict in data_type_modify_list] # get list of dictionaries representing columns that have newly changed dtypes 
        
        # Check if there are new tuple object to a table
        # If there is, append it to non existent columns list 
        for table_column_dictionary_from_current_change_window  in filtered_ddl_tables_from_current_change_window[table_from_current_change_window ]:
            if table_column_dictionary_from_current_change_window  not in filtered_ddl_tables_from_past[table_from_past]:
                if table_column_dictionary_from_current_change_window  in dtype_dictionary_list_from_current_change_window : # continue if change is already covered by data_type_modify_list
                    continue
                print(f"New column detected: {table_from_past}.{table_column_dictionary_from_past['column_name']}.\n>>> {table_column_dictionary_from_current_change_window }.")
                non_existent_columns.append(table_column_dictionary_from_current_change_window)
                non_existent_columns_tables.append(table_from_current_change_window)
    
    output_dictionary = {'new_columns': non_existent_columns,
                         'changed_columns': data_type_modify_list,
                         'new_columns_tables': non_existent_columns_tables}
    return output_dictionary


def generate_alter_statements(core, db, alter_type ,schema_changes, comparison_name = ''):
    query_executor, cursor , connector = initalize_snowflake()
    catalog_insert_template = "INSERT INTO CATALOG.TABLE_FIELD_CATALOG  (TABLE_OWNER, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, COLUMN_DATATYPE) VALUES ('Unknown','{db}','{sch}','{tbl}','{col_name}','{dtype}');"
    add_template = 'ALTER TABLE {db}.{sch}.{tbl} ADD COLUMN {col_name} {dtype};'
    modify_template = '--{col_name} {dtype_from_past} --> {dtype}\nALTER TABLE {db}.{sch}.{tbl} MODIFY COLUMN {col_name} {dtype};'
    base_directory = "utils/snowflake/schema_change/output/ddl_diffs/existing_tables"
    data_catalog_base_directory = f"{base_directory}/data_catalog"
    comparison_dates = comparison_name.split('__')[-1].replace('.sql','')
    comparison_name = comparison_name.replace('SCHEMACHANGE','COLUMN_SCHEMACHANGE')
    path_to_comparison_group = f"{base_directory}/{comparison_dates}"
    path_to_core_group = f"{base_directory}/{comparison_dates}/{core[0]}"
    
    if os.path.exists(path_to_comparison_group):
        pass
    else:
        os.mkdir(path_to_comparison_group)
        
    if os.path.exists(path_to_core_group):
        pass
    else:
        os.mkdir(path_to_core_group)
    if os.path.exists(data_catalog_base_directory):
        pass
    else:
        os.mkdir(data_catalog_base_directory)
    if len(schema_changes['changed_columns']) == 0 | len(schema_changes['new_columns']) == 0:
        return print(f"No changes observed for {core}.\nComparison Identifier: {comparison_name}")
    with open(f"{path_to_core_group}/{comparison_name}",'a') as outputFile:
        
        if alter_type == 'modify':
            for changed_column in schema_changes['changed_columns']:
                current_col = changed_column['column_dict_from_current_change_window']
                past_col = changed_column['column_dict_from_past']
                schema = current_col['schema']
                table = current_col['table']
                column_name = current_col['column_name']
                column_length = current_col['column_length'].replace('(','',1).replace(')','',1) if '((' in current_col['column_length'] else current_col['column_length'] 
                type = f"{current_col['column_data_type']} {column_length}"
                dtype_from_past = f"{past_col['column_data_type']} {past_col['column_length']}"
                alter_statement = modify_template.format(dtype_from_past=dtype_from_past,db=db,sch=schema,tbl=table,col_name=column_name,dtype=type)
                outputFile.write(alter_statement)
                outputFile.write('\n')
        elif alter_type == 'add':
            for new_column in schema_changes['new_columns']:
                schema = new_column['schema']
                table = new_column['table']
                column_name = new_column['column_name']
                column_length = new_column['column_length'].replace('(','',1).replace(')','',1) if '((' in new_column['column_length'] else new_column['column_length'] 
                type = f"{new_column['column_data_type']} {column_length}"
                if query_executor.column_exists_in_snowflake(database=db,schema=schema,table=table,column=column_name):
                    continue
                alter_statement = add_template.format(db=db,sch=schema,tbl=table,col_name=column_name,dtype=type)
                insert_statement = catalog_insert_template.format(db=db,sch=schema,tbl=table,col_name=column_name,dtype=type)
                with open(f"{data_catalog_base_directory}/{comparison_name}",'a') as catalog_file:
                    catalog_file.write(insert_statement)
                    catalog_file.write('\n')
                outputFile.write(alter_statement)
                outputFile.write('\n')

def list_paths(path):
    """
    ## What it does
    Retrieves a dictionary containing core name
    as key and corresponding file path as value
    ***e.g:
    'tabpay': 'data_sources/aws_rds/2022-03-01/tab_ddl_rdspcom2_tabpay_2022-03-02T10_43_42-0700.sql'
    """
    d = {}
    for file in sorted(os.listdir(path)):
        env = path.split('/')[2]
        core = file.split('/')[-1].split('_')[3].upper()
        d[core] = f"{path}/{file}"
    return d


def output_ddl_diffs(past_change_window_path = get_ddl_directories_from_data_source('aws_rds' )['past_output'],
                     current_change_window_path=  get_ddl_directories_from_data_source('aws_rds' )['current_output'],
                     cores=None,
                     db_name=None,
                     file_format=None
                     ):
    
    # Get Dictionaries of {"core" : "file path"}
    # Example:
    # {'walgreens': 'data_sources/aws_rds/2022-03-01/tab_ddl_rdspcom2_walgreens_2022-03-02T10_43_42-0700.sql', ...}
    past_ddl_paths_dict  = list_paths(past_change_window_path)
    current_ddl_paths_dict  = list_paths(current_change_window_path)
    past_change_window_date = past_change_window_path.split('/')[-1]
    current_change_window_date = current_change_window_path.split('/')[-1]

    comparison_name = f"__{past_change_window_date}_vs_{current_change_window_date}"
    for core in past_ddl_paths_dict: # iterate over cores inside given directory
        output_file_comparison_name = f"{CREATED_AT}_{core}_SCHEMACHANGE{comparison_name}.sql"
        if cores:
            if core not in cores:
                print("\nSkipped core ", core)
                continue
        past_change_window_core_path = past_ddl_paths_dict[core] # get old ddl filepath
        try:
            current_change_window_core_path = current_ddl_paths_dict[core] # get new ddl filepath
            print(f"\nComparing File paths \n {past_ddl_paths_dict[core]}\nVS\n{current_ddl_paths_dict[core]}")
        except:
            print(f"\nWARNING: Couldn't find core {core} in current ddl directory.")
            continue
        
        # transforming file content into json format
        ddl_dict_from_past_change_window = get_ddl_dictionary(past_change_window_core_path) 
        ddl_dict_from_current_change_window = get_ddl_dictionary(current_change_window_core_path)
        
        create_new_tables_from_current_change_window(ddl_dict_from_past_change_window,
                                                     ddl_dict_from_current_change_window,
                                                     output_file_comparison_name,
                                                     core=core,
                                                     db_name=db_name,
                                                     file_format=file_format)
        
        # find difference between old and new ddl
        schema_changes = get_schema_changes_between_ddls(ddl_dict_from_past_change_window,
                                                         ddl_dict_from_current_change_window,
                                                         core)

        for changed_table in schema_changes['new_columns_tables']:
            print(f"changed table {changed_table}")
            create_operational_pipe(
                changed_table,
                ddl_dict_from_current_change_window[changed_table],
                output_file_comparison_name,
                core,
                db_name,
                True,
                file_format=file_format
            )

        generate_alter_statements(core,
                                  db='ORACLE_RAW',
                                  alter_type='add',
                                  schema_changes = schema_changes,
                                  comparison_name=output_file_comparison_name
                                )
        generate_alter_statements(core,db='ORACLE_TEST',
                                  alter_type='modify',
                                  schema_changes = schema_changes,
                                  comparison_name=output_file_comparison_name
                                )


def get_client_dict(
  clients_path = get_ddl_directories_from_data_source('aws_rds' )['past_output']
, core = None
):
    li = []
    schema=[]
    table=[]
    columns=[]
    dtype=[]
    clength=[]
    # Get Dictionaries of {"core" : "file path"}
    # Example:
    # {'walgreens': 'data_sources/aws_rds/2022-03-01/tab_ddl_rdspcom2_walgreens_2022-03-02T10_43_42-0700.sql', ...}
    ddl_paths_dict  = list_paths(clients_path)

    core_path = ddl_paths_dict[core] # get old ddl filepath

    # transforming file content into json format
    ddl_dict = get_ddl_dictionary(core_path) 

    for k,v in ddl_dict.items():
        for column in v:
            clength.append(column['column_length'])
            dtype.append(column['column_data_type'])
            columns.append(column['column_name'])
            schema.append(k.split('.')[0])
            table.append(k.split('.')[1])
    df = pd.DataFrame(
        {'SCHEMA_NAME': schema,
        'TABLE_NAME': table,
        'COLUMN_NAME': columns,
        'DATA_TYPE': dtype,
        'LENGTH': clength
        })
    li.append(df)
    frame = pd.concat(li, axis=0, ignore_index=True)
    return frame


def generate_data_catalog_statements(db, cores = None, clients_path=get_ddl_directories_from_data_source('aws_rds' )['past_output']):
    query_executor, cursor , connector = initalize_snowflake()
    data_catalog_base_directory = "utils/snowflake/schema_change/output/data_catalog/new_rows"
    change_window_date = clients_path.split('/')[-1]
    new_rows_list = []
    ddl_rows_list = []
    catalog_insert_template = """INSERT INTO DATA_CATALOG.CATALOG.TABLE_FIELD_CATALOG VALUES"""
    values = []
    if os.path.exists(data_catalog_base_directory):
        pass
    else:
        os.mkdir(data_catalog_base_directory)
    
    for schema in cores:
        print('Executing query')
        new_rows_client=query_executor.get_data_catalog_info(db=db, schema=schema)
        ddl_rows_client=get_client_dict(clients_path = clients_path, core = schema)
        new_rows_list.append(new_rows_client)
        ddl_rows_list.append(ddl_rows_client)

    new_rows = pd.concat(new_rows_list, axis=0, ignore_index=True)
    ddl_rows = pd.concat(ddl_rows_list, axis=0, ignore_index=True)

    ddl_rows["LENGTH_STR"] = ddl_rows["LENGTH"].where(~ddl_rows["LENGTH"].isna(), '')
    ddl_rows["LENGTH_STR"] = ddl_rows['LENGTH_STR'].str.replace('-','')
    ddl_rows["COLUMN_DATATYPE"] = ddl_rows['DATA_TYPE'].where(ddl_rows["LENGTH_STR"] == '',ddl_rows['DATA_TYPE'] +'('+ ddl_rows["LENGTH_STR"]+')')
    
    create_dc = pd.merge(new_rows, ddl_rows, how="inner", on=['TABLE_NAME', 'COLUMN_NAME'])
    create_dc.sort_values(['TABLE_NAME','COLUMN_NAME','MASKING_RULE','IS_PCI','IS_PII','ENCRYPTED_AT_SOURCE'],ascending = [True, True, False, False, False, True], inplace=True)
    create_dc.drop_duplicates(subset=['TABLE_NAME','COLUMN_NAME'],keep='first', inplace=True)
    create_dc['COLUMN_DATATYPE']=create_dc['COLUMN_DATATYPE'].str.replace('\(\(\(','(').str.replace('\)\)\)',')').str.replace('\(\(','(').str.replace('\)\)',')')
    create_dc = create_dc.reset_index(drop=True)

    if len(create_dc)>0:
        for i in range(0,len(create_dc)):
            str_type = ['CHAR', 'TEXT']
            timestamp_type = ['DATE', 'TIMESTAMP']
            number_type = ['NUMBER']

            if create_dc['COLUMN_DATATYPE'][i] != create_dc['COLUMN_DATATYPE_TFC'][i] and create_dc['COLUMN_DATATYPE_TFC'][i]:
                SCHEMA_NAME=create_dc['SCHEMA_NAME'][i]
            else:
                SCHEMA_NAME='ALL'

            if create_dc['MASKING_RULE'][i] not in [1,2]:
                if any(ele in create_dc['COLUMN_DATATYPE'][i] for ele in str_type):
                    create_dc['MASKING_RULE'][i] = 3
                elif any(ele in create_dc['COLUMN_DATATYPE'][i] for ele in timestamp_type):
                    create_dc['MASKING_RULE'][i] = 4
                elif any(ele in create_dc['COLUMN_DATATYPE'][i] for ele in number_type):
                    create_dc['MASKING_RULE'][i] = 5

            values.append("""('{TABLE_NAME}','OLTP','{SCHEMA_NAME}',NULL,NULL,'{COLUMN_NAME}','{COLUMN_DATATYPE}',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,{IS_PII},{IS_PCI},NULL,NULL,NULL,NULL,NULL,{ENCRYPTED_AT_SOURCE},{ENCRYPTION_TYPE},{MASKING_RULE},NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,current_timestamp(),current_timestamp(),{IS_TOKENIZED})""".format(TABLE_NAME=create_dc['TABLE_NAME'][i],
                            SCHEMA_NAME=SCHEMA_NAME,
                            COLUMN_NAME=create_dc['COLUMN_NAME'][i],
                            COLUMN_DATATYPE=create_dc['COLUMN_DATATYPE'][i],
                            IS_PII=str(create_dc['IS_PII'][i]).upper(),
                            IS_PCI=str(create_dc['IS_PCI'][i]).upper(),
                            ENCRYPTED_AT_SOURCE=str(create_dc['ENCRYPTED_AT_SOURCE'][i]).upper(),
                            ENCRYPTION_TYPE='NULL' if str(create_dc['ENCRYPTION_TYPE'][i]) == 'nan' else str(int(create_dc['ENCRYPTION_TYPE'][i])),
                            MASKING_RULE=str(create_dc['MASKING_RULE'][i]).replace('.0',''),
                            IS_TOKENIZED=str(create_dc['IS_TOKENIZED'][i]).upper()))
        values_concat=', \n'.join(str(i) for i in values)
        insert_statement=catalog_insert_template+values_concat+';'
        insert_statement = insert_statement.replace(",nan,",",NULL,").replace(",NAN,",",NULL,").replace(',NONE',',NULL')
        with open(f"{data_catalog_base_directory}/data_catalog_new_fields_"+change_window_date+".sql",'a') as catalog_file:
            catalog_file.write(insert_statement)
            catalog_file.write('\n')
    else:
        print('No fields to add')
