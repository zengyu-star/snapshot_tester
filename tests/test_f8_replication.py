import pytest
import logging
import os
import sys

# 添加项目根目录到 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_helpers import SnapshotSandbox, create_test_file
from dual_runner import ParityValidator

logger = logging.getLogger("TestReplicationInteraction")

@pytest.mark.p1
class TestReplicationInteraction:
    """快照 × 副本数命令交互 (F8-01 ~ F8-02)"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner):
        self.runner = runner
        self.sandbox = SnapshotSandbox(runner, "f8_rep_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    def test_f8_01_setrep_isolation(self, runner):
        """F8-01: setrep 不影响快照中的副本因子"""
        create_test_file(runner, f"{self.sandbox.test_dir}/rep_file.dat")
        # 默认副本通常是 1 或 3，我们通过 stat 获取快照时的副本数
        self.sandbox.create_snapshot("snap_v1")

        runner.run_dual_cmd("-setrep", "5", f"{{TARGET}}{self.sandbox.test_dir}/rep_file.dat")

        res_h, _ = runner.run_dual_cmd("-stat", "%r", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/rep_file.dat")
        # 快照中的副本数不应变为 5
        assert res_h.stdout != "5"

    def test_f8_02_setrep_snapshot_forbidden(self, runner):
        """F8-02: 禁止对快照路径执行 setrep"""
        create_test_file(runner, f"{self.sandbox.test_dir}/no_rep.dat")
        self.sandbox.create_snapshot("snap_v1")

        res_h, res_o = runner.run_dual_cmd("-setrep", "2", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/no_rep.dat")
        assert res_h.returncode != 0
        assert res_o.returncode != 0
        ParityValidator.assert_results_match(res_h, res_o)
