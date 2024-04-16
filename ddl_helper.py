
import os
from datetime import datetime
import pytz

# Global Lambda Functions
DATE_TO_TIMESTAMP_NTZ = lambda x: x.replace(" DATE", " TIMESTAMP_NTZ")
# Global Variables
TZ = pytz.timezone('US/Pacific')
CURRENT_US_PACIFIC_DATE = datetime.now(TZ).strftime('%Y-%m-%d')
CURRENT_US_PACIFIC_DATETIME = datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')
PULL_CLIENT_NAME_FROM_FILE_NAME = lambda file_name: file_name.split('/')[-1].split('create')[0] # from string 'output/first_dump_ddls/aws_rds/Z/ZENDA/ZENDA_2022-07-05_b_create_pipe.sql' outputs 'ZENDA_2022-07-05_b'

def convert_pk_constraint_string_to_list(string):
    li = string.replace('"','').replace('(','').replace(')','').replace(' ','').strip().split(',')
    return li

def make_files_versioned(target_directory, starting_version:int=0):
    file_full_paths = []
    version = starting_version
    print(f"Renaming sql files under directory: {target_directory}")
    
    for (directory_path, directory_names, file_names) in os.walk(target_directory):
        if len(file_names) == 0:
            continue
        for file_name in file_names:
            file_full_paths.append(os.path.join(directory_path, file_name))
    
    file_full_paths = sorted(file_full_paths, key=PULL_CLIENT_NAME_FROM_FILE_NAME)
    for file_full_path in file_full_paths:
        file_name = file_full_path.split('/')[-1]
        #print(file_name)
        renamed_file = f"V1.1.{version}__{file_name}"
        renamed_file_full_path = file_full_path.replace(file_name, renamed_file)
        version += 1
        #print(f"{file_full_path}\n>>\n{renamed_file_full_path}\n ---  ---")
        if file_name.endswith('.sql'):
            os.rename(file_full_path,renamed_file_full_path)

if __name__ == '__main__':
    make_files_versioned('output/first_dump_ddls/aws_rds',0)

def write_ddl_row(ddl_row, file_object, table_name, tokenized_keys):

    try:
        column_name = ddl_row.split(' ')[0]
        key = table_name+"."+column_name.strip()
        #print(key)
        dtype = ddl_row.split(' ')[1]
    except:
        pass
    if key in tokenized_keys:
        ddl_row = ddl_row.replace(dtype, 'VARCHAR(64)').replace(' CHAR)','')
        file_object.write(ddl_row)

        if ',' == dtype.strip().replace(' ','').replace('\t','')[-1]:
            file_object.write(',\n')
    else:
        file_object.write(ddl_row)