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
