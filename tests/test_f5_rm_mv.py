"""
F5: 快照 × rm / mv 命令交互
测试用例 F5-01 (P0)

验证删除操作后，快照中的文件仍然可读。
"""
import pytest
import logging

from test_helpers import SnapshotSandbox, create_test_file
from dual_runner import ParityValidator

logger = logging.getLogger("TestRmMvInteraction")


@pytest.mark.p0
class TestRmMvInteraction:
    """快照 × rm/mv 命令交互 (F5-01)"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner):
        self.runner = runner
        self.sandbox = SnapshotSandbox(runner, "f5_rm_mv_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    @pytest.mark.p0
    def test_f5_01_rm_file_snapshot_preserves(self, runner):
        """F5-01: 赋权 -> 造数 -> 快照A -> rm文件 -> cat .snapshot/A/file -> 验证快照内文件仍可读"""
        original_content = "PRESERVED_CONTENT_F5_01"
        create_test_file(runner, f"{self.sandbox.test_dir}/will_be_deleted.dat", original_content)
        self.sandbox.create_snapshot("snap_v1")

        # 从活跃目录删除文件
        runner.run_dual_cmd("-rm", f"{{TARGET}}{self.sandbox.test_dir}/will_be_deleted.dat")

        # 验证活跃目录中文件已不存在
        res_h_ls, res_o_ls = runner.run_dual_cmd(
            "-ls", f"{{TARGET}}{self.sandbox.test_dir}/will_be_deleted.dat"
        )
        assert res_h_ls.returncode != 0, "文件应已从活跃目录删除"

        # cat 快照中的文件，验证仍可读
        res_h, res_o = runner.run_dual_cmd(
            "-cat", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/will_be_deleted.dat"
        )
        ParityValidator.assert_results_match(res_h, res_o)
        assert res_h.returncode == 0, f"快照中的文件应可读: {res_h.stderr}"
        assert original_content in res_h.stdout, \
            f"快照中的文件内容应为原始值 '{original_content}'，实际: '{res_h.stdout}'"

    @pytest.mark.p1
    def test_f5_02_rm_subdir_snapshot_preserves(self, runner):
        """F5-02: 删除子目录后快照仍保留"""
        runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{self.sandbox.test_dir}/sub_dir")
        create_test_file(runner, f"{self.sandbox.test_dir}/sub_dir/inner.dat")
        self.sandbox.create_snapshot("snap_v1")
        
        runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{self.sandbox.test_dir}/sub_dir")
        
        res_h, _ = runner.run_dual_cmd("-ls", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/sub_dir")
        assert res_h.returncode == 0

    @pytest.mark.p1
    def test_f5_04_mv_rename_diff_r(self, runner):
        """F5-04: mv 目录内重命名产生 R 标记"""
        create_test_file(runner, f"{self.sandbox.test_dir}/old.dat")
        self.sandbox.create_snapshot("snap_v1")
        
        runner.run_dual_cmd("-mv", f"{{TARGET}}{self.sandbox.test_dir}/old.dat", f"{{TARGET}}{self.sandbox.test_dir}/new.dat")
        self.sandbox.create_snapshot("snap_v2")
        
        res_h, _ = runner.run_dual_hdfs_cmd("snapshotDiff", f"{{TARGET}}{self.sandbox.test_dir}", "snap_v1", "snap_v2")
        # 有些 HDFS 环境可能表现为 + 和 -，所以兼容判断
        assert "R" in res_h.stdout or ("+" in res_h.stdout and "-" in res_h.stdout)
