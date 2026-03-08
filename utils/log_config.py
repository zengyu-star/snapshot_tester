import logging
import os
import sys
import datetime
from logging.handlers import RotatingFileHandler

def setup_logging(level=logging.INFO):
    """
    统一的日志配置
    - 控制台输出带颜色高亮、简明的时间和模块信息
    - 文件输出包含更详细的进程/线程及精确时间，带有日志轮转
    """
    logger = logging.getLogger()
    
    # 如果已配置过，直接返回避免重复添加
    if logger.handlers:
        return
        
    logger.setLevel(level)

    # 1. 控制台输出 (简明、高亮)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    # 格式: [2026-03-08 11:22:33] [INFO   ] [DualRunner] - 消息内容
    console_formatter = logging.Formatter(
        fmt='[%(asctime)s] [%(levelname)-7s] [%(name)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # 2. 文件输出 (详细、带轮转)
    # 首先确保 logs 文件夹存在于项目根路径 (这里通过路径推演)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logs_dir = os.path.join(project_root, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    log_file = os.path.join(logs_dir, f"run_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
    
    # 每个文件最大 10MB，保留 5 个备份 (TimedRotating 通常按时间切分，但用户希望独立文件)
    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG) # 文件里尽可能多存信息
    file_formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - [%(processName)s:%(threadName)s] - %(filename)s:%(lineno)d - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger
