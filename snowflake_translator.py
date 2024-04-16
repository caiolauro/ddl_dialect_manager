import re
from datetime import datetime, timedelta
import os
from utils.snowflake.schema_change.snowflake_dialect_manager import OracleDialect


end_of_ddl_file_date = [datetime.now().strftime('%B-%y').upper(), (datetime.now() - timedelta(days=30)).strftime('%B-%y').upper()]
date_to_timestamp_ntz = lambda x: x.replace(" DATE", " TIMESTAMP_NTZ")

def translate_dir(dir_path):
    os.mkdir(dir_path.replace('input','output'))
    files = os.listdir(dir_path)
    print(f"Translating {dir_path}. Containing {len(files)} cores.")
    files_paths = [f"{dir_path}/{file}" for file in files]
    for file_name, file_path in zip(files, files_paths):
        output_file_path = file_path.replace('input','output').replace('.lst','.sql')
        parse_oracle_file(file_path,output_file_path)

def parse_oracle_file(input_file, output_file, db='ORACLE_TEST'):
        date_to_datetime = lambda x: x.replace(" DATE", " DATETIME")
        metadataColumns = "\t(\n\tOPERATION_TYPE VARCHAR(1),\n\tFULLY_QUALIFIED_TABLE_NAME VARCHAR(100),\n\tROW_COMMIT_TIME TIMESTAMP,\n\tROW_EXTRACT_TIME TIMESTAMP,\n\tTRAIL_POSITION VARCHAR(100),\n\tDATA_INGESTION_TIME TIMESTAMP,\n"
        core_name = input_file.split('/')[-1].split('_')[3].upper()
        with open(output_file, 'w', newline='') as sqlfile:
            with open(input_file, "r") as file:
                tablecount=0
                table_name = ""
                next_line_hold = None
                while True:
                        next_line = file.readline().replace('"','')
                        if not next_line: 
                            break
                        if 'NOT NULL ENABLE' in next_line:
                            next_line = next_line.replace(" NOT NULL ENABLE","")
                        if 'NOVALIDATE' in next_line:
                            next_line = next_line.replace(" NOVALIDATE","")
                        if 'CREATE UNIQUE INDEX' in next_line:
                            next_line_hold = True
                            continue  
                        if 'ONTROL ON' in next_line:
                            next_line_hold = True
                            continue  
                        if 'CREATE INDEX' in next_line:
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
                        if 'USING INDEX' in next_line:
                            next_line_hold = True
                            continue
                        if 'ALTER TABLE' in next_line:
                            next_line_hold = True
                            continue
                        if 'USING INDEX' in next_line:
                            next_line_hold = True
                            continue
                        if next_line_hold:
                            next_line_hold = None
                            continue
                        if 'PRIMARY KEY' in next_line:
                            continue
                        if "PARTITION" in next_line and table_name != "":     # skip the like with partition word           
                            if "PARTITION BY RANGE" in next_line and ';' in next_line:
                                sqlfile.write(';')
                            if "PARTITION BY RANGE" in next_line and 'INTERVAL' in next_line:
                                sqlfile.write(';')
                            continue
                        if "LOB (" in next_line:
                            table_name=""
                            sqlfile.write(';\n\n')
                            continue
                        if "ORGANIZATION EXTERNAL" in next_line:
                            table_name= ""
                            sqlfile.write(';\n\n')
                            continue
                        if "ENABLE STORAGE" in next_line:         
                            table_name=""
                            sqlfile.write(';\n\n')
                            continue
                        if "ENABLE ROW MOVEMENT" in next_line:     # skip the like with ENABLE ROW MOVEMENT word           
                            if "ENABLE ROW MOVEMENT" in next_line and ';' in next_line:
                                sqlfile.write(');')
                            continue
                        
                        if "NO INMEMORY" in next_line:     # remove NO INMEMORY        
                            sqlfile.write(next_line.replace("NO INMEMORY","").strip())
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
                            sqlfile.write(replaced_next_line)
                            continue   
                        
                        if " LONG" in next_line and table_name != "":  # replace LONG -> STRING
                            replaced_next_line = next_line.replace(" LONG"," STRING")
                            sqlfile.write(replaced_next_line)
                            continue
                        
                        if " NUMBER(*,0)" in next_line and table_name != "":  # replace (*,0) -> ''
                            
                            sqlfile.write(next_line.replace(r'(*,0)','').replace('(	',''))
                            continue   

                        if "\tSTART " in next_line and table_name != "":  # START -> _START_''
                            sqlfile.write(next_line.replace('\tSTART','\t_START_'))
                            continue   

                        if " CLOB" in next_line and table_name != "":  # replace CLOB -> VARCHAR
                            replaced_next_line = next_line.replace(" CLOB"," VARCHAR")
                            sqlfile.write(replaced_next_line)
                            continue   

                        if " NCLOB" in next_line and table_name != "":  # replace CLOB -> VARCHAR
                            replaced_next_line = next_line.replace(" NCLOB"," VARCHAR")
                            sqlfile.write(replaced_next_line)
                            continue   

                        if " RAW" in next_line and table_name != "":  # replace RAW -> VARIANT
                            #replaced_next_line = next_line.replace(" RAW"," VARIANT")
                            if '$' in next_line:
                                sqlfile.write(re.sub("RAW.*$",repl='VARIANT,',string=next_line).replace('$',''))
                            else:
                                sqlfile.write(re.sub("RAW.*$",repl='VARIANT,',string=next_line))
                            #sqlfile.write(replaced_next_line)
                            continue   
                        
                        if ' ENCRYPT USING' in next_line and table_name != "":
                            if next_line.endswith(',',0 ,-1):
                                #print("removing...",re.sub("ENCRYPT USING.*$",repl=',',string=next_line))
                                sqlfile.write(re.sub("ENCRYPT USING.*$",repl=',',string=next_line))
                            elif next_line.endswith('\n') and next_line.endswith(',',0,-1) == False:
                                #print("removing...",re.sub("ENCRYPT USING.*$",repl='',string=next_line))
                                sqlfile.write(re.sub("ENCRYPT USING.*$",repl='',string=next_line))
                            continue

                        if " DEFAULT" in next_line and table_name != "": # remove DEFAULTS

                            if next_line.endswith(',',0 ,-1):
                                if '(' in next_line[0:5]:
                                    sqlfile.write(date_to_timestamp_ntz(re.sub("DEFAULT.*$",repl=',',string=next_line).replace('(','',1)))
                                else:
                                    sqlfile.write(date_to_timestamp_ntz(re.sub("DEFAULT.*$",repl=',',string=next_line)))
                            elif next_line.endswith('\n') and next_line.endswith(',',0,-1) == False:
                                sqlfile.write(date_to_timestamp_ntz(re.sub("DEFAULT.*$",repl='',string=next_line)))
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
                                sqlfile.write(next_line.replace("$",""))

                        if 'WITH TIME ZONE' in next_line and table_name != "":  # replace WITH TIMEZONE -> TIMESTAMP_TZ
                            replaced_next_line = next_line.replace('WITH TIME ZONE','').replace('TIMESTAMP','TIMESTAMP_TZ')
                            sqlfile.write(replaced_next_line)
                            continue
                        if " DATE" in next_line and table_name != "":  # replace DATE -> DATETIME
                            if '$' in next_line:
                                replaced_next_line = next_line.replace('$','')
                            elif '(' in next_line[0:5]:
                                replaced_next_line = next_line.replace(" DATE"," TIMESTAMP_NTZ").replace('(','')
                            else:
                                replaced_next_line = next_line.replace(" DATE"," TIMESTAMP_NTZ")
                            sqlfile.write(replaced_next_line)
                            continue   
                        
                        if " GENERATED ALWAYS AS" in next_line and table_name != "": # remove VIRTUAL Columns
                            if next_line.endswith(',') or next_line.endswith(',',0,-1):
                                sqlfile.write(re.sub(" GENERATED ALWAYS AS.*$",repl=',',string=next_line))
                            elif next_line.endswith(',') == False or next_line.endswith(',',0,-1) == False:
                                sqlfile.write(re.sub(" GENERATED ALWAYS AS.*$",repl='',string=next_line))

                            continue

                        if "UROWID" in next_line and table_name != "": # replace UROWID(400) -> VARCHAR
                            sqlfile.write(re.sub("UROWID.*$",repl='VARCHAR,',string=next_line))
                            continue

                        if " CHAR)" in next_line and table_name != "": #  CHAR) -> )
                            replaced_next_line = next_line
                            if '(' in next_line[0:5]:
                                replaced_next_line = next_line.replace('(','',1)
                            sqlfile.write(replaced_next_line.replace(' CHAR)',')'))

                            continue

                        if "SYS.XMLTYPE" in next_line and table_name != "": # replace SYS.XMLTYPE -> VARIANT
                            replaced_next_line = next_line.replace("SYS.XMLTYPE","VARIANT")
                            sqlfile.write(replaced_next_line)
                            continue
                        if OracleDialect.end_of_ddl_file_date[0] in next_line:
                                continue
                        elif OracleDialect.end_of_ddl_file_date[1] in next_line:
                            continue
                        if  "CREATE TABLE"  in  next_line: # get tablename
                            table_name = next_line.replace('"','').split(".")[1].strip()                            
                            tablecount+=1  
                            #print(f"{db}.{core_name}.{table_name}")
                            sqlfile.write(next_line.replace("CREATE TABLE","CREATE TABLE IF NOT EXISTS").replace('(','').replace(f"{core_name}.{table_name}",f"{db}.{core_name}.{table_name}"))
                            sqlfile.write("\n")
                            sqlfile.write(metadataColumns) 
                            continue
                        
                        if len(table_name) > 0: 
                            if next_line.strip().startswith('('):
                                next_line=next_line.replace('(','',1)
                            sqlfile.write(next_line)



