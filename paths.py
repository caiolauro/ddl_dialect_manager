import os

from regex import D
from snowflake_translator import parse_oracle_file
class DDLPathManager:
    
    input_ds_path = 'utils/snowflake/schema_change/input/data_sources'
    data_sources = os.listdir(input_ds_path)

    @classmethod
    def create_output_mirror_dirs(cls, dry_mode=False):
        output_paths_dict = {}
        output_dirs_paths = []
        input_dirs_paths = []
        data_sources_output_paths = [f"{cls.input_ds_path}/{data_source}/".replace('input','output') for data_source in cls.data_sources]
        for opath in data_sources_output_paths:
            data_source = opath.split('/')[5].upper()
            if os.path.exists(opath)==False:
                os.mkdir(opath)
            
            ipath = opath.replace('output','input')
            ds_dates = os.listdir(ipath)
            output_ds_paths_to_dates = [f"{opath}{date}" for date in ds_dates]
            input_ds_paths_to_dates = [f"{ipath}{date}" for date in ds_dates]
            output_paths_dict[data_source] = output_ds_paths_to_dates
            for ds_path_to_date in output_ds_paths_to_dates:
                if os.path.exists(ds_path_to_date)==False and dry_mode==False:
                    os.mkdir(ds_path_to_date)
                output_dirs_paths.append(ds_path_to_date)

        return output_paths_dict
    

if __name__ == '__main__':
    DDLPathManager.create_output_dirs()