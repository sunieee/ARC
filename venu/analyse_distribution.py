import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import math

field = os.environ.get('field')

def analyze_csv(filename, column_name):
    # 读取CSV文件
    if filename.endswith('.csv'):
        df = pd.read_csv(filename)
    else:
        # 使用glob找到文件夹下所有的CSV文件
        files = glob.glob(f"{filename}/*.csv")
        
        print('len(files):', len(files))
        # 读取并合并所有CSV文件
        all_dfs = [pd.read_csv(file) for file in files]
        df = pd.concat(all_dfs, ignore_index=True)
    
    # 计算0的比例
    count0 = len(df[df[column_name] == 0])
    # 计算0和1之间的比例
    count = len(df[(df[column_name] > 0) & (df[column_name] < 1)])
    # 计算1的比例
    count1 = len(df[df[column_name] == 1])
    count_greater_than_equal_0_5 = len(df[df[column_name] >= 0.5])

    # 计算均值和方差
    mean = df[column_name].mean()
    variance = df[column_name].var()
    median = df[column_name].quantile(0.5)

    des = f"""{column_name} ratio: 
    0: {round(count0/len(df), 3)}
    0~1: {round(count/len(df), 3)}
    1: {round(count1/len(df), 3)}
    >0.5: {round(count_greater_than_equal_0_5/len(df), 3)}
Mean: {round(mean, 3)}
Variance: {round(variance, 3)}
50% Quantile: {round(median, 3)}"""
    print(des)

    # 绘制分布图
    plt.figure(figsize=(10, 6))
    plt.hist(df[column_name], bins=50, edgecolor='k', alpha=0.7)
    plt.title(f"Distribution of {column_name} in {field}", fontsize=14)
    plt.xlabel(column_name)
    plt.ylabel('Frequency')
    plt.axvline(mean, color='r', linestyle='dashed', linewidth=1)
    plt.text(mean*1.05, plt.ylim()[1]*0.9, 'Mean', rotation=0, color='r', fontsize=14)
    plt.text(mean*1.05, plt.ylim()[1]*0.5, des, color='b', fontsize=14)  # 添加描述信息
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout()

    # 保存分布图
    plt.savefig(f"out/{field}/{column_name}_distribution.png")
    print(f"save distribution to out/{field}/{column_name}_distribution.png")
    # plt.show()

# 使用
analyze_csv(f'out/{field}/papers.csv', 'isKeyPaper')
analyze_csv(f'out/{field}/links.csv', 'extendsProb')

'''
len(files): 103
0 ratio of isKeyPaper: 0.34728365657253035
Not 0/1 ratio of isKeyPaper: 0.09935352160598843
1 ratio of isKeyPaper: 0.5533628218214812
Mean of isKeyPaper: 0.612727694724963
Variance of isKeyPaper: 0.22085644677635208
save distribution to out/scigene_visualization_field/isKeyPaper_distribution.png

len(files): 5948
0 ratio of isKeyPaper: 0.42371147483893434
Not 0/1 ratio of isKeyPaper: 0.08122932265366534
1 ratio of isKeyPaper: 0.4950592025074003
Mean of isKeyPaper: 0.5432543481968676
Variance of isKeyPaper: 0.23407294670970955
save distribution to out/scigene_visualization_field/isKeyPaper_distribution.png
'''