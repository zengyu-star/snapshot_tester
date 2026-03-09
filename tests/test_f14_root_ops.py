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

    def test_f14_03_nested_snapshotable_dirs_independence(self, runner):
        """F14-03: 验证多个 snapshottable 目录的快照独立性"""
        # 由于 HDFS 默认可能禁止嵌套 snapshottable，我们采用两个并列目录来验证独立性
        dir_a = "/f14_03_dir_a"
        dir_b = "/f14_03_dir_b"
        
        # 清理并创建
        runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{dir_a}")
        runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{dir_b}")
        runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{dir_a}")
        runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{dir_b}")
        
        create_test_file(runner, f"{dir_a}/a.txt", "DATA_A")
        create_test_file(runner, f"{dir_b}/b.txt", "DATA_B")
        
        try:
            # 开启快照
            res1, _ = runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{dir_a}")
            assert res1.returncode == 0
            res2, _ = runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{dir_b}")
            assert res2.returncode == 0
            
            # 分别创建快照
            runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{dir_a}", "s_a")
            runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{dir_b}", "s_b")
            
            # 交叉变异验证
            runner.run_dual_cmd("-rm", f"{{TARGET}}{dir_a}/a.txt")
            
            # A 的快照应保留 A 的数据
            res_h, _ = runner.run_dual_cmd("-ls", f"{{TARGET}}{dir_a}/.snapshot/s_a/a.txt")
            assert res_h.returncode == 0
            
            # B 的快照应不受影响
            res_h2, _ = runner.run_dual_cmd("-ls", f"{{TARGET}}{dir_b}/.snapshot/s_b/b.txt")
            assert res_h2.returncode == 0
            
        finally:
            # 清理
            runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{dir_a}", "s_a")
            runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{dir_b}", "s_b")
            runner.run_dual_admin_cmd("-disallowSnapshot", f"{{TARGET}}{dir_a}")
            runner.run_dual_admin_cmd("-disallowSnapshot", f"{{TARGET}}{dir_b}")
            runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{dir_a}")
            runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{dir_b}")
