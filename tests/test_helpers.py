"""
测试共享工具模块：SnapshotSandbox 和文件创建辅助函数。
此模块可被所有测试文件直接 import。
"""
import os
import sys
import logging
import random
import string
import yaml

# 添加项目根目录到 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dual_runner import DualHadoopCommandRunner

logger = logging.getLogger("test_helpers")

# ==================== 路径常量 ====================
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_HOST_TMP_PREFIX = os.path.join(_PROJECT_ROOT, ".tmp_test_data")
_CONTAINER_MOUNT = "/obsa_workspace"


def _host_to_container(host_path):
    """
    智能路径转换：自动兼容 Docker Mock 模式与物理机原生实测模式
    """
    # 1. 检查底层代理脚本是否存在（dual_runner 判断是否走 Docker 的依据）
    local_hdfs_script = os.path.join(_PROJECT_ROOT, "hdfs")
    
    # 2. 读取 config.yml 中的运行模式
    is_mock = False
    config_path = os.path.join(_PROJECT_ROOT, "config.yml")
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                conf = yaml.safe_load(f)
                is_mock = conf.get("global", {}).get("mock_obsa_mode", False)
        except Exception as e:
            logger.warning(f"读取配置失败，默认使用实测模式路径: {e}")

    # 判断：只有当配置为 mock_obsa_mode 且存在的 hdfs docker 代理脚本时，才执行路径转换
    if is_mock and os.path.exists(local_hdfs_script):
        return host_path.replace(_PROJECT_ROOT, _CONTAINER_MOUNT)
    
    # 实测模式下，直接返回宿主机真实绝对路径，供原生的 Hadoop 客户端调用
    return host_path


# ==================== 文件创建工具 ====================

def create_test_file(runner, relative_path, content="TEST_DATA_CONTENT_12345"):
    """在双端创建一个包含指定内容的测试文件。"""
    host_tmp = f"{_HOST_TMP_PREFIX}_{os.getpid()}_{random.randint(0, 99999)}.dat"
    container_tmp = _host_to_container(host_tmp)

    try:
        with open(host_tmp, "w") as f:
            f.write(content)
        res_h, res_o = runner.run_dual_cmd("-put", "-f", container_tmp, f"{{TARGET}}{relative_path}")
        assert res_h.returncode == 0, f"HDFS put 失败: {res_h.stderr}"
        assert res_o.returncode == 0, f"OBS put 失败: {res_o.stderr}"
    finally:
        if os.path.exists(host_tmp):
            os.remove(host_tmp)
    return res_h, res_o


def create_test_file_with_size(runner, relative_path, size_bytes=1024):
    """创建指定大小的测试文件"""
    content = ''.join(random.choices(string.ascii_letters + string.digits, k=size_bytes))
    return create_test_file(runner, relative_path, content)


def create_local_tmp_file(content="APPEND_DATA"):
    """创建本地临时文件并返回 (宿主机路径, 容器内路径)"""
    host_tmp = f"{_HOST_TMP_PREFIX}_{os.getpid()}_{random.randint(0, 99999)}.dat"
    container_tmp = _host_to_container(host_tmp)
    with open(host_tmp, "w") as f:
        f.write(content)
    return host_tmp, container_tmp


def cleanup_local_tmp(host_path):
    """清理临时文件"""
    if os.path.exists(host_path):
        os.remove(host_path)


# ==================== 沙箱管理器 ====================

class SnapshotSandbox:
    """
    快照测试沙箱管理器。
    负责创建/清理测试目录，以及跟踪创建的快照以便 teardown 时清理。
    """

    def __init__(self, runner, test_dir_name):
        self.runner = runner
        self.test_dir = f"/{test_dir_name}"
        self._snapshots = []

    def setup(self):
        """创建干净的沙箱目录"""
        logger.info(f"=== Sandbox Setup: {self.test_dir} ===")
        self.runner.run_dual_cmd("-rm", "-r", "-f", f"{{TARGET}}{self.test_dir}")
        res_h, res_o = self.runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{self.test_dir}")
        assert res_h.returncode == 0, f"Setup mkdir failed: {res_h.stderr}"
        assert res_o.returncode == 0, f"Setup mkdir failed (OBS): {res_o.stderr}"

    def allow_snapshot(self):
        res_h, res_o = self.runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{self.test_dir}")
        assert res_h.returncode == 0, f"allowSnapshot failed: {res_h.stderr}"
        assert res_o.returncode == 0, f"allowSnapshot failed (OBS): {res_o.stderr}"
        return res_h, res_o

    def disallow_snapshot(self):
        res_h, res_o = self.runner.run_dual_admin_cmd("-disallowSnapshot", f"{{TARGET}}{self.test_dir}")
        return res_h, res_o

    def create_snapshot(self, snap_name):
        # 【修复】：操作前先注册追踪，即使后续异常中断，teardown 也会尝试去删除它，保证防漏
        if snap_name not in self._snapshots:
            self._snapshots.append(snap_name)
            
        res_h, res_o = self.runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{self.test_dir}", snap_name)
        assert res_h.returncode == 0, f"createSnapshot {snap_name} failed: {res_h.stderr}"
        assert res_o.returncode == 0, f"createSnapshot {snap_name} failed (OBS): {res_o.stderr}"
        return res_h, res_o

    def delete_snapshot(self, snap_name):
        res_h, res_o = self.runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{self.test_dir}", snap_name)
        assert res_h.returncode == 0, f"deleteSnapshot {snap_name} failed: {res_h.stderr}"
        assert res_o.returncode == 0, f"deleteSnapshot {snap_name} failed (OBS): {res_o.stderr}"
        if snap_name in self._snapshots:
            self._snapshots.remove(snap_name)
        return res_h, res_o

    def teardown(self):
        """严苛清理：删除所有快照 -> 解除快照权限 -> 删除目录"""
        logger.info(f"=== Sandbox Teardown: {self.test_dir} ===")
        for snap in list(self._snapshots):
            self.runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{self.test_dir}", snap)
        self._snapshots.clear()
        self.runner.run_dual_admin_cmd("-disallowSnapshot", f"{{TARGET}}{self.test_dir}")
        self.runner.run_dual_cmd("-rm", "-r", "-f", f"{{TARGET}}{self.test_dir}")
        logger.info(f"=== Sandbox 清理完毕 ===")