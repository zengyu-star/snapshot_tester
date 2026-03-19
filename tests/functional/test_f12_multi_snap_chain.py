import pytest
import logging
import os
import sys

# 添加项目根目录到 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_helpers import SnapshotSandbox, create_test_file, create_local_tmp_file, cleanup_local_tmp


logger = logging.getLogger("TestMultiSnapChain")

@pytest.mark.p2
class TestMultiSnapChain:
    """F12: 多快照时序与交叉操作"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner, validator):
        self.runner = runner
        self.validator = validator
        self.sandbox = SnapshotSandbox(runner, "f12_chain_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    def test_f12_01_snapshot_chain_isolation(self, runner):
        """F12-01: 三快照链各自保留对应时间点的文件快照"""
        create_test_file(runner, f"{self.sandbox.test_dir}/base.txt", "V0")
        self.sandbox.create_snapshot("A")
        
        # 变异 1：新增 file_B
        create_test_file(runner, f"{self.sandbox.test_dir}/file_B.txt", "V1")
        self.sandbox.create_snapshot("B")
        
        # 变异 2：新增 file_C
        create_test_file(runner, f"{self.sandbox.test_dir}/file_C.txt", "V2")
        self.sandbox.create_snapshot("C")
        
        # 验证快照 A 中只有 base.txt，不含 file_B 和 file_C
        res_h, res_o = runner.run_dual_cmd("-ls", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/A/")
        self.validator.assert_results_match(res_h, res_o)
        assert "base.txt" in res_h.stdout
        assert "file_B.txt" not in res_h.stdout
        assert "file_C.txt" not in res_h.stdout
        
        # 验证快照 B 中有 base.txt 和 file_B.txt，不含 file_C
        res_h, res_o = runner.run_dual_cmd("-ls", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/B/")
        self.validator.assert_results_match(res_h, res_o)
        assert "base.txt" in res_h.stdout
        assert "file_B.txt" in res_h.stdout
        assert "file_C.txt" not in res_h.stdout
        
        # 验证快照 C 中三个文件都存在
        res_h, res_o = runner.run_dual_cmd("-ls", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/C/")
        self.validator.assert_results_match(res_h, res_o)
        assert "base.txt" in res_h.stdout
        assert "file_B.txt" in res_h.stdout
        assert "file_C.txt" in res_h.stdout

    def test_f12_02_delete_middle_does_not_break_chain(self, runner):
        """F12-02: 删除历史快照不影响后续快照的可用性"""
        self.sandbox.create_snapshot("S1")
        create_test_file(runner, f"{self.sandbox.test_dir}/f1")
        self.sandbox.create_snapshot("S2")
        
        self.sandbox.delete_snapshot("S1")
        
        # 验证 S2 快照仍然可用且内容完整
        res_h, res_o = runner.run_dual_cmd("-ls", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/S2/")
        self.validator.assert_results_match(res_h, res_o)
        assert res_h.returncode == 0, "删除 S1 后，S2 快照应仍然可用"
        assert "f1" in res_h.stdout, "S2 快照中应包含 f1 文件"
