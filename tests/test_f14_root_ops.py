import pytest
import logging
import os
import sys

# 添加项目根目录到 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_helpers import SnapshotSandbox, create_test_file
from dual_runner import ParityValidator

logger = logging.getLogger("TestRootOps")

@pytest.mark.p2
class TestRootOps:
    """F14: 快照根目录特权与操作限制"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner):
        self.runner = runner
        self.sandbox = SnapshotSandbox(runner, "f14_root_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    def test_f14_01_ls_root_parity(self, runner):
        """F14-01: ls 快照根目录一致性"""
        self.sandbox.create_snapshot("s1")
        res_h, res_o = runner.run_dual_cmd("-ls", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot")
        ParityValidator.assert_results_match(res_h, res_o)

    def test_f14_02_snapshot_path_as_source_for_put_fails(self, runner):
        """F14-02: 禁止将快照路径作为 put 的目标(只读)"""
        # 已在 Lifecycle 和 Block 场景覆盖，此处补强
        self.sandbox.create_snapshot("s1")
        res_h, res_o = runner.run_dual_cmd("-touchz", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/s1/illegal")
        assert res_h.returncode != 0
        assert res_o.returncode != 0
        ParityValidator.assert_results_match(res_h, res_o)
