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
        """F14-03: 验证嵌套 snapshottable 目录的快照独立性"""
        parent = self.sandbox.test_dir
        child = f"{parent}/nested_sub"
        runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{child}")
        create_test_file(runner, f"{child}/data.txt", "CHILD_DATA")
        
        # 父子都开启快照
        runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{parent}")
        runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{child}")
        
        # 分别创建快照
        runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{parent}", "snap_parent")
        runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{child}", "snap_child")
        
        # 变异子目录并验证父目录快照
        runner.run_dual_cmd("-rm", f"{{TARGET}}{child}/data.txt")
        
        # 父目录快照里的子目录内容应该还在 (HDFS 行为：快照是递归的快照)
        res_h, _ = runner.run_dual_cmd("-ls", f"{{TARGET}}{parent}/.snapshot/snap_parent/nested_sub/data.txt")
        assert res_h.returncode == 0
        
        # 子目录快照里也应该还在
        res_h2, _ = runner.run_dual_cmd("-ls", f"{{TARGET}}{child}/.snapshot/snap_child/data.txt")
        assert res_h2.returncode == 0
        
        # 清理以便 teardown
        runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{child}", "snap_child")
        runner.run_dual_admin_cmd("-disallowSnapshot", f"{{TARGET}}{child}")
