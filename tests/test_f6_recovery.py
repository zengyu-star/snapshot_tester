import pytest
import logging
import os
import sys

# 添加项目根目录到 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_helpers import SnapshotSandbox, create_test_file
from dual_runner import ParityValidator

logger = logging.getLogger("TestRecoveryCP")

@pytest.mark.p2
class TestRecoveryCP:
    """F6: 快照 × cp 命令交互（含快照恢复）"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner):
        self.runner = runner
        self.sandbox = SnapshotSandbox(runner, "f6_recovery_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    def test_f6_01_recovery_single_file(self, runner):
        """F6-01: 从快照路径 cp 恢复单文件控制流"""
        content = "RECOVERY_DATA_F6_01"
        create_test_file(runner, f"{self.sandbox.test_dir}/file.dat", content)
        self.sandbox.create_snapshot("snap_v1")
        
        runner.run_dual_cmd("-rm", f"{{TARGET}}{self.sandbox.test_dir}/file.dat")
        
        # 恢复
        res_h, res_o = runner.run_dual_cmd("-cp", 
            f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/file.dat", 
            f"{{TARGET}}{self.sandbox.test_dir}/file_recovered.dat"
        )
        ParityValidator.assert_results_match(res_h, res_o)
        
        # 校验恢复后的内容
        ls_h, _ = runner.run_dual_cmd("-cat", f"{{TARGET}}{self.sandbox.test_dir}/file_recovered.dat")
        assert ls_h.stdout == content

    def test_f6_04_cp_to_snapshot_forbidden(self, runner):
        """F6-04: 禁止向 .snapshot 路径 cp 写入"""
        create_test_file(runner, f"{self.sandbox.test_dir}/src.dat")
        self.sandbox.create_snapshot("snap_v1")
        
        res_h, res_o = runner.run_dual_cmd("-cp", 
            f"{{TARGET}}{self.sandbox.test_dir}/src.dat", 
            f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/target.dat"
        )
        assert res_h.returncode != 0
        assert res_o.returncode != 0
        ParityValidator.assert_results_match(res_h, res_o)
