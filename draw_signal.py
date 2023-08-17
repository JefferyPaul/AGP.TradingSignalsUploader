
import os
import shutil
from datetime import datetime
from collections import defaultdict
import argparse
from time import sleep

import matplotlib.pyplot as plt
import pandas as pd


# ============================
# 设置目标日期，如果不填入，默认最新日期

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('-n', '--newest', help='最新日期', action="store_true")

args = arg_parser.parse_args()
_newest = args.newest


# ============================

p_root = os.path.abspath(os.path.dirname(__file__))
p_data = os.path.join(p_root, 'output')
p_output_data = os.path.join(p_root, 'output_symbol_data')
p_output_pct = os.path.join(p_root, 'output_symbol_pct')
if os.path.isdir(p_output_pct):
    shutil.rmtree(p_output_pct)
    sleep(0.1)
os.makedirs(p_output_pct)
if os.path.isdir(p_output_data):
    shutil.rmtree(p_output_data)
    sleep(0.1)
os.makedirs(p_output_data)

l_target_folder = [
    os.path.join(p_data, i) for i in os.listdir(p_data)
    if os.path.isdir(os.path.join(p_data, i))
]
if _newest:
    l_target_folder = [max(l_target_folder)]       

# 数据读取
print('reading data')
l_data_all = list()
d_data_all_string = defaultdict(list)
d_data_increased = defaultdict(list)
d_last_tp_increased = defaultdict(str)

for path_folder in sorted(l_target_folder):
    print(path_folder)
    l_all_file_name = sorted(os.listdir(path_folder))
    for n, file_name in enumerate(l_all_file_name):
        _last_file = False
        if n+1 == len(l_all_file_name):
            # 需要读取最后一个文件并记录数据
            _last_file = True
        p_file = os.path.join(path_folder, file_name)
        if not os.path.isfile(p_file):
            continue
        with open(p_file) as f:
            l_lines = f.readlines()
        for line in l_lines:
            line = line.strip()
            if line == '':
                continue
            _dt, _ticker, _tp = line.split(',')
            l_data_all.append({"datetime": _dt, "ticker": _ticker, "target": float(_tp)})
            d_data_all_string[_ticker].append(line)
            if not _last_file:
                if d_last_tp_increased[_ticker] == _tp:
                    continue
            d_data_increased[_ticker].append({
                'dt': datetime.strptime(_dt, '%Y%m%d %H%M%S'),
                'tp': float(_tp)
            })
            d_last_tp_increased[_ticker] = _tp

# [1] 数据
for _ticker, _data in d_data_all_string.items():
    p_output = os.path.join(p_output_data, f'{_ticker}.csv')
    print(f'output data file, {_ticker}')
    with open(p_output, 'w') as f:
        f.writelines('\n'.join(_data))
df_all = pd.DataFrame(l_data_all)
# print(df_all.head())
df_pivot = pd.pivot_table(df_all, values='target', index='datetime', columns='ticker',)
df_pivot.to_csv(os.path.join(p_output_data, '_all.csv'), header=True)


# [2] 画图
d_df = {}
for k, v in d_data_increased.items():
    df = pd.DataFrame(v)
    df.set_index('dt', inplace=True,)
    d_df[k] = df


# 画图
print('\n')
for k, _df in d_df.items():
    fig = plt.figure(figsize=(10, 10),)
    ax = fig.add_subplot(211)
    ax.set_title(k)
    ax.plot(_df, drawstyle="steps-post")
    ax = fig.add_subplot(212)
    ax.plot(_df['tp'].to_list(), drawstyle="steps-post")
    # _df.plot(drawstyle="steps-post", title=f'{k}')
    # _df.plot(use_index=False, drawstyle="steps-post", title=f'{k} - s')
    plt.savefig(os.path.join(p_output_pct, k + '.png'))
    print(f'saved {k}')
