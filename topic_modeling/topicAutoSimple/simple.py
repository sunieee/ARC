import os
import pandas as pd 
import numpy as np
import bisect
import math
import shutil
import paramiko
import json
import logging

from datetime import datetime

def initLog(fileName):
    logger = logging.getLogger(fileName)
    logger.setLevel(logging.INFO)
    
    fh = logging.FileHandler(fileName, mode='a+')
    fh.setLevel(logging.INFO)
    
    formatter = logging.Formatter('[%(asctime)s]: %(message)s')
    fh.setFormatter(formatter)
    
    logger.addHandler(fh)
    
    return logger

def removeMidFile(field):
    dirs = [f'{d}/{field}' for d in ['output', 'model']]
    for tarDir in dirs:
        shutil.rmtree(tarDir)


def saveMidFile(field, topicCount, topicSize, save2Version=False):
    dirs = ['output', 'model']
    
    for tarDir in dirs:  
        os.makedirs(f'./version/{tarDir}', exist_ok=True)
        nowTime = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
        shutil.copytree(f'./{tarDir}/{field}', f'./version/{tarDir}/{field}-{topicSize}-{topicCount}_{nowTime}')
        
        if save2Version:
            if os.path.exists(f'./version/{tarDir}/{field}'):
                shutil.rmtree(f'./version/{tarDir}/{field}')
                
            shutil.move(f'./{tarDir}/{field}', f'./version/{tarDir}')
        else:
            shutil.rmtree(f'./{tarDir}/{field}')

def loadResFile(field):
    dirs = ['output', 'model']
    # dirs = [f'{d}/{field}' for d in ['output', 'model']]
    for tarDir in dirs:
        shutil.move(f'./version/{tarDir}/{field}', f'./{tarDir}')

"""
    Codes between --- --- are copied from GPT-4
    No need to change them until something wrong happens
"""
# ------------------------------------------------------------------
def execute_command_as_root(ssh, command, sudo_password):
    """ Execute a single command as root using sudo and the provided password """
    command = f"sudo -S -p '' {command}"
    stdin, stdout, stderr = ssh.exec_command(command)
    stdin.write(sudo_password + "\n")
    stdin.flush()
    return stdout.readlines(), stderr.readlines()

def upload_file_as_root(field):
    server = '82.156.152.182'
    port = 22
    username = 'xl'
    password = 'QWer!@34'
    sudo_password = 'QWer!@34'
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server, port, username, password)

    # Create directory as root if needed
    srcBaseDir = f'./output/{field}'
    tarBaseDir = f'/home/xfl/pyCode/GFVisTest/csv/{field}'
    
    stdout, stderr = execute_command_as_root(ssh, f"mkdir -p {tarBaseDir}", sudo_password)
    if stderr:
        print("Error in creating directory:", stderr)
        return

    files = ['paperID2topic.json', 'field_leaves.csv', 'field_roots.csv'] 
    
    for file in files:
        sftp = ssh.open_sftp()
        
        # Temporarily upload file to a user-writable location
        tmpName = f"/tmp/{file}"
        sftp.put(f'{srcBaseDir}/{file}', tmpName)
        sftp.close()

        # Move file to the final location as root
        _, stderr = execute_command_as_root(ssh, f"mv {tmpName} {tarBaseDir}/{file}", sudo_password)
        if stderr:
            print("Error in moving file:", stderr)

    ssh.close()
    
    def upload_file_as_root(server, port, username, password, sudo_password, local_filepath, remote_filepath):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server, port, username, password)

        # Create directory as root if needed
        directory = remote_filepath.rsplit('/', 1)[0]
        stdout, stderr = execute_command_as_root(ssh, f"mkdir -p {directory}", sudo_password)
        if stderr:
            print("Error in creating directory:", stderr)
            return

        # Temporarily upload file to a user-writable location
        temp_location = f"/tmp/{local_filepath.rsplit('/', 1)[-1]}"
        sftp = ssh.open_sftp()
        sftp.put(local_filepath, temp_location)
        sftp.close()

        # Move file to the final location as root
        _, stderr = execute_command_as_root(ssh, f"mv {temp_location} {remote_filepath}", sudo_password)
        if stderr:
            print("Error in moving file:", stderr)

        ssh.close()
    #-----------------------------------------------------------

def uploadFile(field):
    
    # srcBaseDir = f'./output/{field}'
    # tarBaseDir = f'/home/xfl/pyCode/GFVisTest/csv/{field}'
    # files = ['paperID2topic.json', 'field_leaves.csv', 'field_roots.csv']
    # for file in files:
    #     upload_file_as_root('82.156.152.182', 22, 'xl', 'QWer!@34', 'QWer!@34', '{srcBaseDir}/{file}', '{tarBaseDir}/{file}')
    
    # upload_file_as_root('82.156.152.182', 22, 'xl', 'QWer!@34', 'QWer!@34', './topicAutoSimple/src/log.txt', '/home/xfl/tmp/tmp0/tmp1/log.txt')
    
    upload_file_as_root(field)


if __name__ == '__main__':
    srcDir = './src'
    logDir = './log'
    
    norLogger = initLog(f'{logDir}/log.txt')
    sumLogger = initLog(f'{logDir}/summary.txt')
    errLogger = initLog(f'{logDir}/errors.txt')
    
    with open(f'{srcDir}/excludeField.txt', 'r') as f:
            exFields = set(s.strip() for s in f.readlines())
        
    with open(f'{srcDir}/finishedField.txt', 'r') as f:
        for s in f.readlines():
            exFields.add(s.strip())
            
    with open(f'{srcDir}/topicCountRatio.json', 'r') as f:
        topicCountRatio = json.load(f)
    
    import sys
    fieldDir = '/home/sy/MAGProcessing/create_field/out'
    
    if len(sys.argv) > 1 and sys.argv[1] == 'XFL':
        fieldDir = '/home/xfl/PyProject/MAGProcessing/create_field/out'
    
    print(fieldDir)
    fieldList = []
    if os.path.exists(f'{srcDir}/todo.txt'):
        with open(f'{srcDir}/todo.txt', 'r') as f:
            fieldList = [s.strip() for s in f.readlines()]
 
    if len(fieldList) <= 0:
        fieldList = os.listdir(fieldDir)
        
    
    for field in fieldList:
        if os.path.exists(f'./src/papers_{field}_autoTop.csv'):
            df = pd.read_csv(f'./src/papers_{field}_autoTop.csv')
        else:
            
            if exFields.__contains__(field) or not os.path.isdir(f'{fieldDir}/{field}'):
                print(os.path.isdir(f'{fieldDir}/{field}'))
                continue
            if not os.path.exists(f'{fieldDir}/{field}/papers_top.csv'):
                continue
            
            df = pd.read_csv(f'{fieldDir}/{field}/papers_top.csv')
            df = df[['paperID', 'title', 'abstract']]

            df.to_csv(f'./src/papers_{field}_autoTop.csv', index=False)
            
        # shutil.copyfile(f'{fieldDir}/{field}/papers_top.csv', f'./src/papers_{field}_top.csv')
        
        # fieldList.append(field)
        
        totalLen = len(df)
        noAbLen = df.isna().sum()['abstract']
        hasAbLen = totalLen - noAbLen
        
        hasAbRatio = hasAbLen / totalLen       
            
        # with open(f'{srcDir}/summary.txt', 'a+') as f:
        sumLogger.info(f'field: {field}, paperCount: {totalLen}, Has-abstract count: {hasAbLen}, Has-abstract ratio: {hasAbLen / totalLen}')

        if hasAbRatio < 0.6 or hasAbLen < 40000:
            # with open(f'{srcDir}/fieldExceptions.txt', 'a+') as f:
            #     if hasAbRatio < 0.6:
            #         f.write(f'{field} papers having abstract Ratio: {hasAbRatio}')
            #     if hasAbLen < 40000:
            #         f.write(f'{field} papers having abstract Count: {hasAbLen}')
                    
            if hasAbRatio < 0.6:
                errLogger.warning(f'{field} papers having abstract Ratio: {hasAbRatio}')
            if hasAbLen < 40000:
                errLogger.warning(f'{field} papers having abstract Count: {hasAbLen}')
                
            if hasAbLen < 10000:
                errLogger.error(f'{field} papers having abstract Count: {hasAbLen}')
                continue
            
        # firstly set a lower topicSize bound for each field
        
        # paperCountSteps = np.array([ii * 10000 for ii in [1, 5, 10, 50, 100, 200, 500, 1000]])
        # baseTopicSizes = np.array([10, 40, 80, 120, 200, 250, 350, 500, 800])
        # baseTopicCounts = np.array([20, 30, 40, 50, 60, 70, 80, 90, 100])
        
        # boundIndex = bisect.bisect_left(paperCountSteps, hasAbLen)
        # minTopicSize = baseTopicSizes[boundIndex]
        # baseTopicCount = baseTopicCounts[boundIndex]
        
        ratio = topicCountRatio[field] if topicCountRatio.__contains__(field) else 1
        if hasAbLen <= 100000:
            baseTopicCount = 80
            minTopicSize = 100
        else:
            baseTopicCount = (80 + 30 * math.log(hasAbLen / 100000) + 1)
            minTopicSize = (int(100 * (1 + math.log(hasAbLen / 100000))) + hasAbLen / 10000)
        
        baseTopicCount = int(baseTopicCount * ratio)
        minTopicSize = int(minTopicSize / ratio)
        
        sumLogger.info(f'\tbaseTopicCount: {baseTopicCount}, startTopicSize: {minTopicSize}')
        norLogger.info(f'Start learing field {field}')
             
        lowerTSBound = -1
        upperTSBound = math.inf
        
        lowerTopicCount = 15
        finalTopicCount = -1
        lastTopicSize = 0
        modelSaved = False
        
        iterTimes = 1
        while lowerTSBound == -1 or upperTSBound == math.inf:
            norLogger.info(f'\tTo find the bounds, start running field {field} for {iterTimes} times')
            ret = os.system(f'bash geneTopics.sh {field} {minTopicSize} {lowerTopicCount}')
            if ret < 0:
                exit(-1)
            df = pd.read_csv(f'./output/{field}/topic_count_name.csv')
            topicCount = len(df)
            norLogger.info(f'\tRunning field {field} for {iterTimes} times completed.')
            norLogger.info(f'\t\tlowerTSBound: {lowerTSBound}, upperTSBound: {upperTSBound} minTopicSize: {minTopicSize}, topicCount: {topicCount}, baseTopicCount: {baseTopicCount}')
        
            gapRatio = topicCount / baseTopicCount - 1
            
            if abs(gapRatio) < 0.05:
                if lastTopicSize < minTopicSize:
                    finalTopicCount = topicCount
                    lastTopicSize = minTopicSize
                    saveMidFile(field, topicCount, minTopicSize, save2Version=True)
                    modelSaved = True
                else:
                    saveMidFile(field, topicCount, minTopicSize, save2Version=False)
            else:
                removeMidFile(field)
            
            if abs(gapRatio) < 0.15:
                
                if gapRatio >= 0:
                    # topicSize just little smaller, but I'm going to find the larger topicSize, so Just set the lower Bound
                    # It may cause the failure of topic model learning, so set the minTopicSize a little bit larger
                    lowerTSBound = max(minTopicSize, lowerTSBound)
                    minTopicSize = int(minTopicSize * 1.5)
                    
                elif gapRatio < 0:
                    # topicSize just little larger, but I'm going to find the larger topicSize, so set the upper bound a little larger
                    upperTSBound = min(int(minTopicSize * (1.2 + gapRatio)), upperTSBound)
                    minTopicSize = int(minTopicSize / 1.5)
                    lowerTopicCount = max(topicCount, lowerTopicCount)
                   
            elif topicCount < baseTopicCount:
                # topicSize too large
                upperTSBound = min(minTopicSize, upperTSBound)
                minTopicSize //= 2
                lowerTopicCount = max(int(topicCount * 0.75), lowerTopicCount)
            else:
                lowerTSBound = max(minTopicSize, lowerTSBound)
                minTopicSize *= 2
            
            iterTimes += 1
            
        iterTimes = 1
        while lowerTSBound <= upperTSBound:
            minTopicSize = (lowerTSBound + upperTSBound) // 2
            norLogger.info(f'\tTo find the best topic count, start running field {field} for {iterTimes} times')
            ret = os.system(f'bash geneTopics.sh {field} {minTopicSize} {lowerTopicCount}')
            if ret < 0:
                exit(-1)
            
            df = pd.read_csv(f'./output/{field}/topic_count_name.csv')
            topicCount = len(df)
            
            norLogger.info(f'\tTo find the best topic count, run field {field} for {iterTimes} times')
            norLogger.info(f'\t\tlowerTSBound: {lowerTSBound}, upperTSBound: {upperTSBound} minTopicSize: {minTopicSize}, topicCount: {topicCount}')
                
            
            gapRatio = topicCount / baseTopicCount - 1
            
            if abs(gapRatio) < 0.05:
                if lastTopicSize < minTopicSize:
                    finalTopicCount = topicCount
                    lastTopicSize = minTopicSize
                    saveMidFile(field, topicCount, minTopicSize, save2Version=True)
                    modelSaved = True
                else:
                    saveMidFile(field, topicCount, minTopicSize, save2Version=False)
            else:
                removeMidFile(field)
            
            if abs(gapRatio) < 0.1:
                if gapRatio >= 0:
                    # topicSize just little smaller, but I'm going to find the larger topicSize, so Just set the lower Bound
                    lowerTSBound = max(minTopicSize, lowerTSBound)
                    
                elif gapRatio < 0:
                    # topicSize just little larger, but I'm going to find the larger topicSize, so set the upper bound a little larger
                    upperTSBound = min(int(minTopicSize * (1.2 + gapRatio)), upperTSBound)
 
            elif topicCount < baseTopicCount:
                upperTSBound = minTopicSize
                lowerTopicCount = max(int(topicCount * 0.75), lowerTopicCount)
            else:
                lowerTSBound = minTopicSize
            
            
            if iterTimes > 4 and modelSaved:
                break
            
            iterTimes += 1
            
        sumLogger.info(f'\ttopicSize: {lowerTSBound}, finalTopicCount: {finalTopicCount}, baseTopicCount: {baseTopicCount}')
        
        loadResFile(field)
        norLogger.info(f'Finish learing field {field}')
        os.system(f'bash geneOthers.sh {field}')
        norLogger.info(f'Finish generating others for field {field}')
        uploadFile(field)
        norLogger.info(f'Finish uploading files for field {field}\n')
        
        with open(f'{srcDir}/finishedField.txt', 'a+') as f:
            f.write(field + '\n')
        
        
        