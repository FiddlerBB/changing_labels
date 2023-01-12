import pandas as pd 
import glob
import os
import shutil
import luigi

FOLDER_NAME = 'data_robotflow'
OUTPUT_PATH = 'output/luigi_pipline/{}/labels'.format(FOLDER_NAME)
INPUT_PATH = 'input/{}/labels/*.txt'.format(FOLDER_NAME)

REPLACE_DICT = {
    0: 2,
    1: 3,
    2: 0,
    3: 1
}


class MakeFolder(luigi.Task):
    def output(self):
        return luigi.LocalTarget(OUTPUT_PATH)

    def run(self):
        os.makedirs(OUTPUT_PATH, exist_ok=True)

class ChangeLabel(luigi.Task):
    def requires(self):
        return MakeFolder()

    def output(self):
        return [luigi.LocalTarget(os.path.join(OUTPUT_PATH, os.path.basename(f))) for f in glob.glob(INPUT_PATH)]

    def run(self):
        for file in glob.glob(INPUT_PATH):
            df = pd.read_csv(file, header=None, sep = ' ')
            df = df[0].map(lambda x: REPLACE_DICT.get(x, x))
            final_path = os.path.join(OUTPUT_PATH, os.path.basename(file))
            df.to_csv(final_path, index=False, header=None, sep = ' ')


luigi.build([ChangeLabel()], local_scheduler=True)
    
    