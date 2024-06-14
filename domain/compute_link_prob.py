import pickle
import pandas as pd
from sklearn.impute import SimpleImputer
import numpy as np
import os

field = os.environ.get('field')

# 1. 加载模型
with open('saved_model.pickle', 'rb') as file:
    model = pickle.load(file)

# 2. 读取测试数据
df = pd.read_csv(f'out/{field}/all_features.csv')

# 3. 数据预处理
features = ["cross_correlation","window_cross_correlation","year_diff","citing_paper_citationcount","cited_paper_citationcount","self_cite","similarity","jaccard_cocitation","jaccard_bibcoupling"]
df_name = df[["citingpaperID", "citedpaperID", "authorID"]]
df_test = df[features]

# 处理缺失值
imp = SimpleImputer(missing_values=np.nan, strategy="constant", fill_value=-2)
df_test = pd.DataFrame(imp.fit_transform(df_test))

# 4. 使用模型进行预测
result = model.predict_proba(df_test)[:, 1]

# 5. 合并预测结果并保存
result_df = pd.DataFrame(result, columns=['proba'])
edge = pd.concat([df_name, result_df], axis=1)
edge.to_csv(f'out/{field}/edge_proba.csv', index=None)
