"""
读取配置

循环
    检查是否处于运行时间段（若否 continue）
    检测1（错误阻塞并弹框）： 长时间未上传新文件，请查看原因
    查找当天的目标文件（一直找，找到为止）
    读取文件（错误continue循环）。 使用"try-except"，读取失败报错，但不阻塞（如果多次读取失败，则会在循环的”检测1“中弹框警告并阻塞）

    检查文件内容（行数）。空文件，直接跳过；行数变少，跳过；行数只能增加或者不变。

    上传。  可配置

报错


"""

import os
import datetime
import shutil
import threading
import time
from logging import Logger
from typing import List
from collections import defaultdict, namedtuple

# import sys
# PATH_FILE = os.path.abspath(__file__)
# PATH_PROJECT = os.path.dirname(os.path.dirname(PATH_FILE))
# sys.path.append(PATH_PROJECT)

from ..helper.scheduler import ScheduleRunner
from ..helper.tp_WarningBoard import run_warning_board
from ..helper.tp_MessageClient import send_file

"""
一个实例 可服务 多个trader的signal，但是只能输出一个 target file 并上传到 message server

单线程运行，会阻塞；
主程序使用 while 和 sleep；
    当检查到 日期 更新，则更改 signal file 的地址，并等待至这些文件均已生成为止；
    
"""

SignalCache = namedtuple(
    'SignalCache',
    [
        'TraderName', 'DateTime', 'Ticker',
        'Price', 'TargetPosition', 'Direction',
    ]
)

SIGNAL_CACHE_HEADER = [
    'TraderName', 'DateTime', 'Ticker', 'TraderName2',
    '_unknown_5', 'Price', 'TargetPosition', 'Direction',
    'OrderType', '_Speculation', '_unknown_11', '_unknown_12',
    '_unknown_13', '_unknown_14', '_unknown_15', 'DateTime2', '_unknown_17'
]


class SignalUploader(ScheduleRunner):
    def __init__(
            self, paths: list, logger: Logger,
            key, mc_ip, mc_port, output_root,
            interval: float, running_time: list,
            target_len: int,    # 信号文件行数，也就是信号trader数一定要完全一致
            stop_timeout: int = 60            # 超时，引发暂停运行
    ):
        """

        :param paths:
        :param logger:
        """
        super(SignalUploader, self).__init__(running_time=running_time, loop_interval=30)
        self._uploader_thread: None or threading.Thread = None  # 只能运行一个 start线程

        self.root_paths = paths
        self.logger = logger
        self._upload_key = key
        self._mc_ip = mc_ip
        self._mc_port = mc_port
        self._output_root = os.path.abspath(output_root)
        self._output_tp_file = os.path.join(self._output_root, 'TargetPosition.csv')
        self._interval = interval
        self.target_len = target_len
        self.stop_timeout = stop_timeout

        # cache
        # 目标 signal cache 文件
        self._d_signal_files_path: dict = {}
        self._last_signals = {}         # 用于记录旧signal, 检查 新旧 signal  的差异
        # 当前日期，str，用于查找最新目标文件
        self._signal_date: datetime.date = datetime.datetime.now().date()

    @property
    def signal_files_path(self) -> dict:
        return self._d_signal_files_path

    def _running_loop(self):
        """
        因为 SignalCache 文件 是 严格的 一天一个的，
        所以只需要在 日期 变动时，重新检查 目标文件 即可。
        :return:
        """
        dt_last_upload: datetime.datetime = datetime.datetime.now()
        self._signal_date: datetime.date = datetime.datetime.now().date()
        self._gen_newest_signal_file_path(interval=10)  # 会等待至所有文件均已生成

        while self._schedule_in_running:
            # [0]检查上一次的上传时间
            # 如果长时间未上传新文件, 则报错, 并阻塞,
            gap = (datetime.datetime.now() - dt_last_upload).seconds
            if gap >= self.stop_timeout:
                self.logger.error('长时间未上传新文件，请查看原因。 超时时长： %s ' % gap)
                _my_exception(warning_string='长时间未上传新文件')
                dt_last_upload = datetime.datetime.now()

            # [1]检查是否新的一天，若是则 检查并等待 当天的最新的信号文件
            # 会阻塞
            if self._signal_date != datetime.datetime.now().date() or len(self._d_signal_files_path) == 0:
                self._gen_newest_signal_file_path(interval=10)  # 会等待至所有文件均已生成

            # print(self.root_paths)
            # print(self._d_signal_files_path)

            # 再次检查 是否继续运行
            if not self._schedule_in_running:
                break

            # [2]读取 SignalCache 数据
            l_file_lines = []
            _read_error = False
            for file_root, file_path in self._d_signal_files_path.items():
                f_data: list or None = self._read_signal_file(file_path, file_root)
                if f_data:
                    l_file_lines += f_data
                else:
                    _read_error = True
                    # 读取文件出错, 文件不存在 或 数据行异常
                    break
            if _read_error:
                # 直接跳过 剩下环节, 重新循环并处理
                time.sleep(self._interval)
                continue
            if len(l_file_lines) == 0:
                self.logger.error('文件为空文件')
                time.sleep(self._interval)
                continue
            elif len(l_file_lines) != self.target_len:
                self.logger.error(f'信号文件行数错误, 目标行数 {self.target_len}, 实际行数 {len(l_file_lines)}')
                time.sleep(self._interval)
                continue

            # [3]解析 SignalCache 数据
            l_signal_cache: List[SignalCache] = self._parse_signal_file(l_file_lines)
            if not l_signal_cache:
                # 直接跳过 剩下环节, 重新循环并处理
                self.logger.warning('解析SignalCache为空')
                time.sleep(self._interval)
                continue

            # 再次检查 是否继续运行
            if not self._schedule_in_running:
                break

            # [4]生成 合并 target,并输出文件
            self._gen_target_data_file(signal_cache=l_signal_cache)

            # [5]上传
            # 上传前备份
            bak_tp_file = os.path.join(
                self._output_root,
                datetime.datetime.now().strftime('%Y%m%d'),
                datetime.datetime.now().strftime('%H%M%S.%f') + '.csv'
            )
            if not os.path.isdir(os.path.dirname(bak_tp_file)):
                os.makedirs(os.path.dirname(bak_tp_file))
            shutil.copyfile(src=self._output_tp_file, dst=bak_tp_file)
            dt_upload: datetime.datetime or None = send_file(
                mc_ip=self._mc_ip,
                mc_port=self._mc_port,
                upload_name=self._upload_key,
                path_target_file=self._output_tp_file,
                timeout=2,
                logger=self.logger,
            )
            if dt_upload:
                dt_last_upload = dt_upload
                # self.logger.info('上传成功: %s' % dt_last_upload.strftime('%H:%M:%S'))
                self.logger.info('上传成功')

            # [6]
            time.sleep(self._interval)

    def _start(self):
        """
        非阻塞
        因为 SignalCache 文件 是 严格的 一天一个的，
        所以只需要在 日期 变动时，重新检查 目标文件 即可。
        :return:
        """
        t = threading.Thread(target=self._running_loop)
        t.start()

    def _end(self):
        # 阻塞,确保仅有一个 线程 在运行
        # 直接用 ScheduleRunner._schedule_in_running 来判断 和 控制，不需要另外 结束
        self.logger.info('正在等待 uploader 线程结束...')
        if self._uploader_thread:
            self._uploader_thread.join()
        self.logger.info('\tuploader 线程已终止')

    # 更新 目标 文件
    def _gen_newest_signal_file_path(self, interval=5):
        self.logger.info('查找目标文件')
        while True:
            l_exist = []
            for path_root in self.root_paths:
                target_file_name = '%s_SignalMap.csv' % datetime.datetime.now().strftime('%Y%m%d')
                target_file_path = os.path.join(path_root, target_file_name)
                if os.path.isfile(target_file_path):
                    l_exist.append(True)
                    self._d_signal_files_path[path_root] = target_file_path
                else:
                    l_exist.append(False)

            if False not in l_exist:
                self.logger.info('新文件已生成')
                break
            elif not self._schedule_in_running:
                self.logger.info('gen_newest_signal_file_path被终止')
                break
            else:
                time.sleep(interval)

    # 读取信号文件
    def _read_signal_file(self, path, path_root) -> list or None:
        # 原文件检查
        try:
            with open(path, encoding='utf-8') as f:
                f_data: list = f.readlines()
            # f_mtime = os.stat(path).st_mtime
        except:
            self.logger.error('OpenError,未正常打开文件.  %s' % datetime.datetime.now().strftime('%H:%M:%S'))
            # 因为此处读取文件错误时，会直接跳过，不暂停 不弹窗，
            # 使用 check_upload_time() 防止长时间没有正常上传
            return None

        # 检查 文件 行数
        data_len = len(f_data)
        if data_len == 0:
            # 直接返回,
            # 不继续运行
            self.logger.error('SignalMap.csv 为空文件，跳过')
            return None
        return f_data

    # 解析信号文件
    def _parse_signal_file(self, data: List[str]) -> List[SignalCache] or None:
        l_all_signal_cache = []
        for s_line in data:
            try:
                # 读取行数据
                s_line = s_line.strip()
                if s_line == '':
                    continue
                d_single_value = {}  # 单个(行)数据
                for n, s_text in enumerate(s_line.split(',')):
                    d_single_value[SIGNAL_CACHE_HEADER[n]] = s_text
                if '.' in d_single_value['DateTime']:
                    dt_signal = datetime.datetime.strptime(d_single_value['DateTime'], '%Y-%m-%d %H:%M:%S.%f')
                else:
                    dt_signal = datetime.datetime.strptime(d_single_value['DateTime'], '%Y-%m-%d %H:%M:%S')
                a_signal_cache = SignalCache(
                    TraderName=d_single_value['TraderName'],
                    DateTime=dt_signal,
                    Ticker=d_single_value['Ticker'],
                    Price=float(d_single_value['Price']),
                    TargetPosition=float(d_single_value['TargetPosition']),
                    Direction=d_single_value['Direction']
                )
                l_all_signal_cache.append(a_signal_cache)
            except:
                self.logger.warning('SignalMap.csv数据解析失败，数据有误: %s' % s_line)
                # _my_exception(warning_string='SignalMap.csv数据解析失败')
                return None

        if not l_all_signal_cache:
            return None

        # 数据判断
        d_single_time = defaultdict(int)
        for signal_cache in l_all_signal_cache:
            s_dt = signal_cache.DateTime.strftime('%m/%d   %H:%M')
            d_single_time[s_dt] += 1
        # self.logger.info(
        #     '信号更新  (共 %s)    %s' % (
        #         str(len(l_all_signal_cache)),
        #         '    '.join([
        #             '( %s  :  %s )' % (str(item[0]), str(item[1]))
        #             for item in sorted(d_single_time.items())
        #         ])
        #     )
        # )
        print('\n'*3)
        print('====================================================')
        self.logger.info(f'信号更新  (共 {str(len(l_all_signal_cache))})')
        for item in sorted(d_single_time.items()):
            _dt = str(item[0])
            _count = str(item[1])
            self.logger.info(f'( {_dt}  :  {_count} )')

        # 检查 新signal 与 旧signal 的差异
        if self._last_signals:
            _last_signals = {}
            count_change = 0
            count_new = 0
            for signal_cache in l_all_signal_cache:
                k = '%s_%s' % (signal_cache.TraderName, signal_cache.Ticker)
                new_tp = '%s %s' % (signal_cache.Direction, str(signal_cache.TargetPosition))
                if k in self._last_signals.keys():
                    old_tp = self._last_signals.pop(k)
                    if new_tp != old_tp:
                        count_change += 1
                else:
                    count_new += 1
                _last_signals[k] = new_tp
            count_old = len(self._last_signals)
            if count_change:
                self.logger.info(f'策略Target更新,  {count_change}')
            self._last_signals = _last_signals
        else:
            for signal_cache in l_all_signal_cache:
                k = '%s_%s' % (signal_cache.TraderName, signal_cache.Ticker)
                new_tp = '%s %s' % (signal_cache.Direction, str(signal_cache.TargetPosition))
                self._last_signals[k] = new_tp

        return l_all_signal_cache

    # 计算 生成 target
    def _gen_target_data_file(self, signal_cache: List[SignalCache]):
        d_tickers_tp = defaultdict(float)  # {ticker: tp, }
        for a_signal_cache in signal_cache:
            # if a_signal_cache.TargetPosition:
            if a_signal_cache.Direction == 'Short':
                d_tickers_tp[a_signal_cache.Ticker] += -a_signal_cache.TargetPosition
            else:
                d_tickers_tp[a_signal_cache.Ticker] += a_signal_cache.TargetPosition

        d_tickers_tp = dict(sorted(d_tickers_tp.items(), reverse=False))

        # 输出：
        time_sign = datetime.datetime.now().strftime('%Y%m%d %H%M%S')
        with open(self._output_tp_file, 'w', encoding='utf-8') as f:
            for ticker_name, target_position in d_tickers_tp.items():
                f.write('%s,%s,%s\n' % (time_sign, ticker_name, target_position))
        # self.logger.info('以生成target文件')


# 调用 warning board 弹框报错。会阻塞
def _my_exception(warning_string):
    assert type(warning_string) is str
    run_warning_board(warning_string + ', 程序已阻塞')
    s_input = input('输入 "OK" 继续运行:\t')
    if s_input.lower() == 'ok':
        return
    else:
        raise Exception

