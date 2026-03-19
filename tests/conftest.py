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

from dual_runner import DualHadoopCommandRunner, ParityValidator
from data_mutator import DataMutator, StressMutator

class ObsaAwareDualRunner(DualHadoopCommandRunner):
    """
    OBSA 感知路由代理：
    拦截管理类命令，强制 OBS 侧使用 hdfs:// 挂载点协议；常规命令保持 obs:// 不变。
    """
    def __init__(self, hdfs_base, obs_base, obs_admin_base, config):
        super().__init__(hdfs_base, obs_base, config)
        self.obs_admin_base = obs_admin_base
    def run_dual_admin_cmd(self, action: str, *args):
        hdfs_cmd = self.admin_cli + [action]
        obs_cmd = self.admin_cli + [action]
        for arg in args:
            if "{TARGET}" in arg:
                hdfs_cmd.append(arg.replace("{TARGET}", self.hdfs_base))
                # 【核心修改】管理员命令目标替换为挂载点路由 (hdfs://)
                obs_cmd.append(arg.replace("{TARGET}", self.obs_admin_base))
            else:
                hdfs_cmd.append(arg)
                obs_cmd.append(arg)
        return self._execute(hdfs_cmd, "hdfs"), self._execute(obs_cmd, "obs")


@pytest.fixture(scope="session")
def config():
    """加载项目配置（整个测试会话只加载一次）"""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(project_root, "config.yml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="session")
def runner(config):
    """创建智能双端执行引擎（整个测试会话共享）"""
    hdfs_base = config['cluster_env']['hdfs_base_uri'].rstrip('/') + "/hdfs_side"
    obs_base = config['cluster_env']['obs_base_uri'].rstrip('/') + "/obs_side"
    obs_admin_base = config['cluster_env']['obs_admin_uri'].rstrip('/') + "/obs_side"
    
    r = ObsaAwareDualRunner(hdfs_base, obs_base, obs_admin_base, config)
    # Mock 模式下不存在真实 OBS 桶，admin 路径必须与 mock 转换后的 data 路径一致，
    # 否则 allowSnapshot 和 createSnapshot 会作用在不同的 HDFS 目录上。
    if r.mock_mode:
        r.obs_admin_base = r.obs_base
    return r


@pytest.fixture(scope="session")
def validator(runner):
    """创建结果校准器，感知 mock 模式"""
    return ParityValidator(is_mock_mode=runner.mock_mode)


@pytest.fixture(scope="session")
def mutator(runner, config):
    """创建造数器（整个测试会话共享）"""
    return StressMutator(runner, config.get("data_model", {}))


# ==================== 日志增强 Hook ====================
import logging
test_logger = logging.getLogger("TestStatus")

@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_logreport(report):
    """
    将每个用例的最终执行结果 (PASSED/FAILED) 写入日志文件
    """
    yield
    if report.when == "call":
        status = report.outcome.upper()
        nodeid = report.nodeid
        test_logger.info(f"TEST RESULT: [{status}] - {nodeid}")
        if report.failed:
            test_logger.error(f"TEST FAILURE DETAIL: {report.longreprtext}")