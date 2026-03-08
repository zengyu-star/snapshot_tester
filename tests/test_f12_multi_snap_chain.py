import pytest
import logging
import os
import sys

# 添加项目根目录到 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_helpers import SnapshotSandbox, create_test_file, create_local_tmp_file, cleanup_local_tmp
from dual_runner import ParityValidator

logger = logging.getLogger("TestMultiSnapChain")

@pytest.mark.p2
class TestMultiSnapChain:
    """F12: 多快照时序与交叉操作"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner):
        self.runner = runner
        self.sandbox = SnapshotSandbox(runner, "f12_chain_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    def test_f12_01_diff_accumulation(self, runner):
        """F12-01: 三快照差异链的累积正确性"""
        create_test_file(runner, f"{self.sandbox.test_dir}/base.txt", "V0")
        self.sandbox.create_snapshot("A")
        
        # 变异 1
        create_test_file(runner, f"{self.sandbox.test_dir}/file_B.txt", "V1")
        self.sandbox.create_snapshot("B")
        
        # 变异 2
        create_test_file(runner, f"{self.sandbox.test_dir}/file_C.txt", "V2")
        self.sandbox.create_snapshot("C")
        
        # 验证 A->B 有 file_B
        res_ab, _ = runner.run_dual_hdfs_cmd("snapshotDiff", f"{{TARGET}}{self.sandbox.test_dir}", "A", "B")
        assert "file_B.txt" in res_ab.stdout
        assert "file_C.txt" not in res_ab.stdout
        
        # 验证 B->C 有 file_C
        res_bc, _ = runner.run_dual_hdfs_cmd("snapshotDiff", f"{{TARGET}}{self.sandbox.test_dir}", "B", "C")
        assert "file_C.txt" in res_bc.stdout
        assert "file_B.txt" not in res_bc.stdout
        
        # 验证 A->C 有两者
        res_ac, _ = runner.run_dual_hdfs_cmd("snapshotDiff", f"{{TARGET}}{self.sandbox.test_dir}", "A", "C")
        assert "file_B.txt" in res_ac.stdout
        assert "file_C.txt" in res_ac.stdout

    def test_f12_02_delete_middle_does_not_break_chain(self, runner):
        """F12-02: 删除历史快照不影响后续快照 diff"""
        self.sandbox.create_snapshot("S1")
        create_test_file(runner, f"{self.sandbox.test_dir}/f1")
        self.sandbox.create_snapshot("S2")
        
        self.sandbox.delete_snapshot("S1")
        
        res_h, res_o = runner.run_dual_hdfs_cmd("snapshotDiff", f"{{TARGET}}{self.sandbox.test_dir}", "S2", ".")
        ParityValidator.assert_results_match(res_h, res_o)
