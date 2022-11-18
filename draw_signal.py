
import os
from datetime import datetime
from collections import defaultdict
import argparse

import matplotlib.pyplot as plt
import pandas as pd


# ============================
# 设置目标日期，如果不填入，默认最新日期

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('-n', '--new', help='最新日期', action="store_true")

args = arg_parser.parse_args()
_newest = args.new


# ============================

p_root = os.path.abspath(os.path.dirname(__file__))
p_data = os.path.join(p_root, 'output')
p_output = os.path.join(p_root, 'output_pct')
if not os.path.isdir(p_output):
    os.makedirs(p_output)

l_target_folder = [
    os.path.join(p_data, i) for i in os.listdir(p_data)
    if os.path.isdir(os.path.join(p_data, i))
]
if _newest:
    l_target_folder = [max(l_target_folder)]       
    

# 数据读取
print('reading data')
d_data = defaultdict(list)
d_last_tp = defaultdict(str)

for path_folder in l_target_folder:
    print(path_folder)
    for file_name in os.listdir(path_folder):
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
            if d_last_tp[_ticker] == _tp:
                continue
            else:
                d_data[_ticker].append({
                    'dt': datetime.strptime(_dt, '%Y%m%d %H%M%S'), 
                    'tp': float(_tp)
                })
                d_last_tp[_ticker] = _tp

d_df = {}
for k, v in d_data.items():
    df = pd.DataFrame(v)
    df.set_index('dt', inplace=True,)
    d_df[k] = df


# 画图
print('\n\n')
for k, _df in d_df.items():
    fig = plt.figure(figsize=(10, 10),)
    ax = fig.add_subplot(211)
    ax.set_title(k)
    ax.plot(_df, drawstyle="steps-post")
    ax = fig.add_subplot(212)
    ax.plot(_df['tp'].to_list(), drawstyle="steps-post")
    # _df.plot(drawstyle="steps-post", title=f'{k}')
    # _df.plot(use_index=False, drawstyle="steps-post", title=f'{k} - s')
    plt.savefig(os.path.join(p_output, k + '.png'))
    print(f'saved {k}')
