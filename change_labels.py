import pandas as pd 
import glob
import os
import shutil


folder_name = 'data_robotflow'
output_path = 'output/{}/labels'.format(folder_name)
input_path = 'input/{}/labels/*.txt'.format(folder_name)

def create_folder(new_path):
    if not os.path.exists(new_path):
        os.makedirs(new_path)
    else:
        shutil.rmtree(new_path)
        os.makedirs(new_path)


def change_label(input_path, output_path, replace_dict):
    for file in glob.glob(input_path):
        name = os.path.basename(file)
        df = pd.read_csv(file, header=None, sep = ' ')
        df = df.replace({0: replace_dict})
        final_path = os.path.join(output_path, name)
        df.to_csv(final_path, index=False, header=None, sep = ' ')

replace_dict ={
    0: 2,
    1: 3,
    2: 0,
    3: 1
}

create_folder(output_path)
change_label(input_path, output_path, replace_dict)
