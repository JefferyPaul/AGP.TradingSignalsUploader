import os
import datetime
import json
from pprint import pprint

import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from TradingSignalsUploader.helper.logger import MyLogger
from TradingSignalsUploader.uploader import SignalUploader

#
P_PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
P_CONFIG = os.path.join(P_PROJECT, 'Config', 'Config.json')
P_OUTPUT_ROOT = os.path.join(P_PROJECT, 'output')
P_LOG_ROOT = os.path.join(P_PROJECT, 'logs')

if not os.path.isdir(P_OUTPUT_ROOT):
    os.makedirs(P_OUTPUT_ROOT)
if not os.path.isdir(P_LOG_ROOT):
    os.makedirs(P_LOG_ROOT)


def main():
    # 读取参数
    with open(P_CONFIG, encoding='utf-8') as f:
        d_config = json.loads(f.read())
    name = d_config['name']
    loop_interval = float(d_config['loopInterval'])  # 运行循环的时间间隔
    signal_cache_root_path = d_config['signalCacheRootPath']  # 目标文件的地址
    message_client_ip = d_config['messageClientIP']  # 上传 ip
    message_client_port = d_config['messageClientPort']  # 上传 port
    upload_name = d_config['uploadName']
    running_time = d_config['runningTime']
    target_len = int(d_config['targetLen'])
    stop_timeout = int(d_config['stopTimeout'])

    # 判断并处理输入的路径
    assert type(signal_cache_root_path) is list
    if len(signal_cache_root_path) == 1:
        # 判断输入的是否为根目录
        _the_root_path = signal_cache_root_path[0]
        assert os.path.isdir(_the_root_path)
        _l = [os.path.join(_the_root_path, _) for _ in os.listdir(_the_root_path)]
        if False in [os.path.isdir(_) for _ in _l]:
            pass
        else:
            signal_cache_root_path = _l.copy()
    pprint(signal_cache_root_path, indent=4)

    # 转换运行时间的格式
    assert type(running_time) is list
    running_time = [
        [
            datetime.datetime.strptime(i[0], '%H%M%S').time(),
            datetime.datetime.strptime(i[1], '%H%M%S').time()
        ]
        for i in running_time
    ]

    # 启动
    logger = MyLogger(name=name, output_root=P_LOG_ROOT)
    logger.info('Started...  %s' % datetime.datetime.now().strftime('%Y%m%d %H:%M:%S'))

    obj = SignalUploader(
        paths=signal_cache_root_path,
        logger=logger,
        key=upload_name,
        mc_ip=message_client_ip,
        mc_port=message_client_port,
        output_root=P_OUTPUT_ROOT,
        interval=loop_interval,
        running_time=running_time,
        target_len=target_len,
        stop_timeout=stop_timeout
    )
    obj.start_loop()

    logger.info('End...  %s' % datetime.datetime.now().strftime('%Y%m%d %H:%M:%S'))


if __name__ == "__main__":
    main()
