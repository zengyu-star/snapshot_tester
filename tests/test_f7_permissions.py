import pytest
import logging
import os
import sys

# 添加项目根目录到 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_helpers import SnapshotSandbox, create_test_file
from dual_runner import ParityValidator

logger = logging.getLogger("TestPermissionsIsolation")

@pytest.mark.p1
class TestPermissionsIsolation:
    """快照 × 权限命令隔离性 (F7-01 ~ F7-05)"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner):
        self.runner = runner
        self.sandbox = SnapshotSandbox(runner, "f7_perm_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    def test_f7_01_chmod_isolation(self, runner):
        """F7-01: chmod 不影响快照中文件权限"""
        create_test_file(runner, f"{self.sandbox.test_dir}/perm_file.dat")
        runner.run_dual_cmd("-chmod", "600", f"{{TARGET}}{self.sandbox.test_dir}/perm_file.dat")
        self.sandbox.create_snapshot("snap_v1")

        runner.run_dual_cmd("-chmod", "777", f"{{TARGET}}{self.sandbox.test_dir}/perm_file.dat")

        res_h, _ = runner.run_dual_cmd("-ls", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/perm_file.dat")
        # 权限位在 ls 输出的前几位，验证快照里还是 rw------- (对应 600)
        assert "rw-------" in res_h.stdout

    def test_f7_03_chmod_snapshot_forbidden(self, runner):
        """F7-03: 禁止对快照路径执行 chmod"""
        create_test_file(runner, f"{self.sandbox.test_dir}/no_chmod.dat")
        self.sandbox.create_snapshot("snap_v1")

        res_h, res_o = runner.run_dual_cmd("-chmod", "777", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/no_chmod.dat")
        assert res_h.returncode != 0
        assert res_o.returncode != 0
        ParityValidator.assert_results_match(res_h, res_o)
