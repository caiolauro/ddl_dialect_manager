import os
import pathlib
import shutil

def extract_files_from_folders(): 
    """
    This function basically extracts files which are living 
    in multiple folder inside a directory, and move them to 
    a desired directory.
    Note: It needs to be executed from the same direcotry where it belongs. 
    """
    currentDirObject = pathlib.Path().cwd()
    currentDirName = currentDirObject.__str__()
    src_folder_name = input("SOURCE: Folder containing clients folders (e.g: DDL-10-5-22)\n>>")
    dst_folder_name = input("DESTINY: Folder to receive clients DDL files (e.g: 2022-05-03_files)\n>>")
    srcDir = f"{currentDirName}/{src_folder_name}"
    dstDir = f"{currentDirName}/{dst_folder_name}"
    os.mkdir(dstDir)
    foldersPathsList = [f"{srcDir}/{folder}" for folder in os.listdir(srcDir)]
    srcFilePathList = [f"{folderPath}/{os.listdir(folderPath)[0]}" for folderPath in foldersPathsList]

    for folderPath in srcFilePathList: 
        shutil.move(dst=dstDir,src=folderPath)
    print(f"{len(srcFilePathList)} files moved successfully.")
