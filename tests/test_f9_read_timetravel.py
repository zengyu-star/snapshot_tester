"""
F9: 快照 × 只读命令交互 (cat / get / tail / checksum / stat)
测试用例 F9-01 (P0)

验证从 .snapshot 路径读取数据的正确性，确保时间旅行语义。
"""
import pytest
import logging

from test_helpers import SnapshotSandbox, create_test_file, create_local_tmp_file, cleanup_local_tmp
from dual_runner import ParityValidator

logger = logging.getLogger("TestReadTimeTravel")


class TestReadTimeTravel:
    """快照 × 只读命令时间旅行 (F9-01)"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner):
        self.runner = runner
        self.sandbox = SnapshotSandbox(runner, "f9_read_tt_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    def test_f9_01_cat_snapshot_shows_original(self, runner):
        """F9-01: 赋权 -> 造数 -> 快照A -> 变异 -> cat .snapshot/A/file -> 验证内容是变异前原始值"""
        original_content = "ORIGINAL_BEFORE_MUTATION_F9_01"
        create_test_file(runner, f"{self.sandbox.test_dir}/time_travel.dat", original_content)
        self.sandbox.create_snapshot("snap_v1")

        # 变异 1: 追加写入
        host_tmp, container_tmp = create_local_tmp_file("_MUTATION_APPENDED")
        try:
            runner.run_dual_cmd(
                "-appendToFile", container_tmp,
                f"{{TARGET}}{self.sandbox.test_dir}/time_travel.dat"
            )
        finally:
            cleanup_local_tmp(host_tmp)

        # 验证活跃文件已包含追加内容
        res_h_active, _ = runner.run_dual_cmd(
            "-cat", f"{{TARGET}}{self.sandbox.test_dir}/time_travel.dat"
        )
        assert "_MUTATION_APPENDED" in res_h_active.stdout, "活跃文件应包含追加内容"

        # cat 快照中的文件 -> 应该只有原始内容
        res_h, res_o = runner.run_dual_cmd(
            "-cat", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/time_travel.dat"
        )
        ParityValidator.assert_results_match(res_h, res_o)
        assert res_h.stdout == original_content, \
            f"快照时间旅行: 期望 '{original_content}'，实际 '{res_h.stdout}'"
        assert "_MUTATION_APPENDED" not in res_h.stdout, \
            "快照中的文件不应包含变异后追加的内容"
