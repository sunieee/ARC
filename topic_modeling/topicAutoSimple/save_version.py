import shutil
import os
import datetime
import sys

current_time = str(datetime.datetime.now())
current_time_lst = current_time.split(' ')
v = current_time_lst[0][2:] + '_' + current_time_lst[1][0:8]
if len(sys.argv) == 2:
    field = sys.argv[1]
    directory = ["output"]
    
    target_dir = os.path.join("version", v)
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    for source_dir in directory:
        shutil.move(f'{source_dir}/{field}', f'{target_dir}/{source_dir}/{field}')
    
    os.makedirs(f"./version/{v}/model")
    shutil.move(f"./model/{field}_model", f"./version/{v}/model/{field}_model")
else:
    directory = ["output", "model", "paper"]
    target_dir = os.path.join("version", v)
    if os.path.exists(target_dir) == False:
        os.mkdir(target_dir)

    for source_dir in directory:
        shutil.move(source_dir, target_dir)