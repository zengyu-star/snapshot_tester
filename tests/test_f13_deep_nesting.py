import pytest
import logging
import os
import sys

# 添加项目根目录到 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_helpers import SnapshotSandbox, create_test_file
from dual_runner import ParityValidator

logger = logging.getLogger("TestDeepNesting")

@pytest.mark.p2
class TestDeepNesting:
    """F13: 深层嵌套路径下的快照鲁棒性"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner):
        self.runner = runner
        self.sandbox = SnapshotSandbox(runner, "f13_deep_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    def test_f13_01_deep_path_snapshot(self, runner):
        """F13-01: 对 10 层以上的深层路径创建快照"""
        deep_path = self.sandbox.test_dir + "/d1/d2/d3/d4/d5/d6/d7/d8/d9/d10"
        runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{deep_path}")
        create_test_file(runner, f"{deep_path}/leaf.txt", "DEEP_CONTENT")
        
        # 对深层路径开启快照并创建 (注意：HDFS 要求 snapshottable 目录即便深层也是可以的)
        runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{deep_path}")
        runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{deep_path}", "snap_deep")
        
        res_h, res_o = runner.run_dual_cmd("-cat", f"{{TARGET}}{deep_path}/.snapshot/snap_deep/leaf.txt")
        ParityValidator.assert_results_match(res_h, res_o)

    def test_f13_02_multiple_subdirs_independent_mutations(self, runner):
        """F13-02: 多个子目录各自创建快照后的独立性"""
        dir1 = f"{self.sandbox.test_dir}/d1_mut"
        dir2 = f"{self.sandbox.test_dir}/d2_mut"
        runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{dir1}")
        runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{dir2}")
        create_test_file(runner, f"{dir1}/f1.txt", "D1")
        create_test_file(runner, f"{dir2}/f2.txt", "D2")
        
        runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{dir1}")
        runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{dir2}")
        
        # S1 for D1
        runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{dir1}", "S1")
        # S2 for D2
        runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{dir2}", "S2")
        
        # 变异并验证隔离
        runner.run_dual_cmd("-rm", f"{{TARGET}}{dir1}/f1.txt")
        res_h, _ = runner.run_dual_cmd("-ls", f"{{TARGET}}{dir1}/.snapshot/S1/f1.txt")
        assert res_h.returncode == 0
        
        # 验证 D2 不受影响
        res_h2, _ = runner.run_dual_cmd("-ls", f"{{TARGET}}{dir2}/f2.txt")
        assert res_h2.returncode == 0

    def test_f13_03_wide_directory_batch_rm(self, runner):
        """F13-03: 宽目录（大量文件）删除后的快照一致性"""
        wide_dir = f"{self.sandbox.test_dir}/wide"
        runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{wide_dir}")
        for i in range(20):
            create_test_file(runner, f"{wide_dir}/file_{i}.txt", f"VAL_{i}")
        
        self.sandbox.create_snapshot("snap_wide")
        
        # 批量删除
        runner.run_dual_cmd("-rm", f"{{TARGET}}{wide_dir}/*.txt")
        
        # 验证快照
        res_h, res_o = runner.run_dual_cmd("-count", f"{{TARGET}}{wide_dir}/.snapshot/snap_wide")
        ParityValidator.assert_results_match(res_h, res_o)
        # 检查一个具体文件
        res_file, _ = runner.run_dual_cmd("-cat", f"{{TARGET}}{wide_dir}/.snapshot/snap_wide/file_10.txt")
        assert res_file.stdout == "VAL_10"


    def test_f13_04_deep_rename_interaction(self, runner):
        """F13-04: 深层路径重命名后的快照一致性"""
        runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{self.sandbox.test_dir}/deep_src/a/b/c")
        create_test_file(runner, f"{self.sandbox.test_dir}/deep_src/a/b/c/f.txt")
        self.sandbox.create_snapshot("S1")
        
        runner.run_dual_cmd("-mv", f"{{TARGET}}{self.sandbox.test_dir}/deep_src", f"{{TARGET}}{self.sandbox.test_dir}/deep_dst")
        
        res_h, _ = runner.run_dual_cmd("-ls", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/S1/deep_src/a/b/c/f.txt")
        assert res_h.returncode == 0
