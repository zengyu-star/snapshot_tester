"""
Shared pytest fixtures for OBSA snapshot tests.
pytest 自动发现此文件，无需手动 import。
"""
import pytest
import yaml
import os
import sys

# 添加项目根目录和 tests 目录到 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dual_runner import DualHadoopCommandRunner
from data_mutator import DataMutator


@pytest.fixture(scope="session")
def config():
    """加载项目配置（整个测试会话只加载一次）"""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(project_root, "config.yml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="session")
def runner(config):
    """创建双端执行引擎（整个测试会话共享）"""
    nn = config['cluster_env']['hadoop_namenode'].rstrip('/')
    base = config['cluster_env']['test_base_path'].strip('/')
    hdfs_test_base = f"{nn}/{base}/hdfs_side"
    obs_test_base = f"obs://{config['cluster_env']['obs_bucket']}/{base}/obs_side"
    return DualHadoopCommandRunner(hdfs_test_base, obs_test_base, config)


@pytest.fixture(scope="session")
def mutator(runner, config):
    """创建造数器（整个测试会话共享）"""
    return DataMutator(runner, config.get("data_model", {}))


# ==================== 日志增强 Hook ====================
import logging
test_logger = logging.getLogger("TestStatus")

@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_logreport(report):
    """
    将每个用例的最终执行结果 (PASSED/FAILED) 写入日志文件
    """
    yield
    # 我们只关心 call 阶段（用例正文执行阶段）的结果
    if report.when == "call":
        status = report.outcome.upper()
        nodeid = report.nodeid
        test_logger.info(f"TEST RESULT: [{status}] - {nodeid}")
        if report.failed:
            test_logger.error(f"TEST FAILURE DETAIL: {report.longreprtext}")
