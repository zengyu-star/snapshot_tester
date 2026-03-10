"""
F3: 快照 × 文件写入命令交互 (put / touchz / appendToFile / copyFromLocal)
测试用例 F3-01, F3-02, F3-03, F3-05 (P0)

验证快照创建后，写入类命令对活跃目录的操作不会穿透到历史快照。
"""
import pytest
import logging
import os

from test_helpers import SnapshotSandbox, create_test_file, create_local_tmp_file, cleanup_local_tmp


logger = logging.getLogger("TestWriteIsolation")


@pytest.mark.p0
class TestWriteIsolation:
    """快照 × 写入命令隔离性 (F3-01, F3-02, F3-03, F3-05)"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner, validator):
        self.runner = runner
        self.validator = validator
        self.sandbox = SnapshotSandbox(runner, "f3_write_iso_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    @pytest.mark.p0
    def test_f3_01_put_not_visible_in_snapshot(self, runner):
        """F3-01: 赋权 -> 快照A -> put新文件 -> ls .snapshot/A -> 验证新文件不可见"""
        self.sandbox.create_snapshot("snap_v1")

        # 快照后 put 新文件
        create_test_file(runner, f"{self.sandbox.test_dir}/new_after_snap.dat")

        # ls 快照内容
        res_h, res_o = runner.run_dual_cmd(
            "-ls", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/"
        )
        self.validator.assert_results_match(res_h, res_o)

        # 新文件不应出现在快照中
        assert "new_after_snap.dat" not in res_h.stdout, \
            f"put 后的文件不应出现在快照中: {res_h.stdout}"

    @pytest.mark.p0
    def test_f3_02_touchz_not_visible_in_snapshot(self, runner):
        """F3-02: 赋权 -> 快照A -> touchz新文件 -> ls .snapshot/A -> 验证新文件不可见"""
        self.sandbox.create_snapshot("snap_v1")

        # 快照后 touchz 新空文件
        runner.run_dual_cmd("-touchz", f"{{TARGET}}{self.sandbox.test_dir}/touched_file.dat")

        # ls 快照内容
        res_h, res_o = runner.run_dual_cmd(
            "-ls", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/"
        )
        self.validator.assert_results_match(res_h, res_o)

        assert "touched_file.dat" not in res_h.stdout, \
            f"touchz 后的文件不应出现在快照中: {res_h.stdout}"

    @pytest.mark.p0
    def test_f3_03_put_overwrite_snapshot_preserves_original(self, runner):
        """F3-03: 赋权 -> 造数 -> 快照A -> put覆盖已有文件 -> cat .snapshot/A/file -> 验证内容是原始值"""
        original_content = "ORIGINAL_CONTENT_F3_03"
        create_test_file(runner, f"{self.sandbox.test_dir}/overwrite_me.dat", original_content)
        self.sandbox.create_snapshot("snap_v1")

        # 用不同内容覆盖
        create_test_file(runner, f"{self.sandbox.test_dir}/overwrite_me.dat", "OVERWRITTEN_NEW_CONTENT")

        # cat 快照中的文件，验证仍是原始内容
        res_h, res_o = runner.run_dual_cmd(
            "-cat", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/overwrite_me.dat"
        )
        self.validator.assert_results_match(res_h, res_o)
        assert original_content in res_h.stdout, \
            f"快照中的文件应保持原始内容，实际: {res_h.stdout}"

    @pytest.mark.p0
    def test_f3_05_append_snapshot_preserves_original(self, runner):
        """F3-05: 赋权 -> 造数 -> 快照A -> appendToFile -> cat .snapshot/A/file -> 验证内容是追加前原始值"""
        original_content = "ORIGINAL_CONTENT_F3_05"
        create_test_file(runner, f"{self.sandbox.test_dir}/append_me.dat", original_content)
        self.sandbox.create_snapshot("snap_v1")

        # 追加内容
        host_tmp, container_tmp = create_local_tmp_file("_EXTRA_APPENDED_DATA")
        try:
            res_h_append, res_o_append = runner.run_dual_cmd(
                "-appendToFile", container_tmp,
                f"{{TARGET}}{self.sandbox.test_dir}/append_me.dat"
            )
            assert res_h_append.returncode == 0, f"HDFS 追加失败: {res_h_append.stderr}"
            assert res_o_append.returncode == 0, f"OBSA 追加失败: {res_o_append.stderr}"
        finally:
            cleanup_local_tmp(host_tmp)

        # cat 快照中的文件，验证仍是原始内容（不含追加部分）
        res_h, res_o = runner.run_dual_cmd(
            "-cat", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/append_me.dat"
        )
        self.validator.assert_results_match(res_h, res_o)
        assert res_h.stdout == original_content, \
            f"快照中文件应只包含原始内容 '{original_content}'，实际: '{res_h.stdout}'"

    @pytest.mark.p1
    def test_f3_06_multiple_append_isolation(self, runner):
        """F3-06: 多次追加后快照间内容隔离"""
        create_test_file(runner, f"{self.sandbox.test_dir}/multi_append.dat", "V1")
        self.sandbox.create_snapshot("snap_v1")
        
        host_tmp, container_tmp = create_local_tmp_file("_V2")
        try:
            res_h_append, res_o_append = runner.run_dual_cmd("-appendToFile", container_tmp, f"{{TARGET}}{self.sandbox.test_dir}/multi_append.dat")
            assert res_h_append.returncode == 0, f"HDFS 追加失败: {res_h_append.stderr}"
            assert res_o_append.returncode == 0, f"OBSA 追加失败: {res_o_append.stderr}"
        finally:
            cleanup_local_tmp(host_tmp)
            
        self.sandbox.create_snapshot("snap_v2")

        res_h1, _ = runner.run_dual_cmd("-cat", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/multi_append.dat")
        res_h2, _ = runner.run_dual_cmd("-cat", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v2/multi_append.dat")
        assert res_h1.stdout == "V1"
        assert res_h2.stdout == "V1_V2"

    @pytest.mark.p1
    def test_f3_07_append_then_rm_snapshot_preserved(self, runner):
        """F3-07: 追加后删除，快照仍保留原始版本"""
        create_test_file(runner, f"{self.sandbox.test_dir}/rm_test.dat", "ORIG")
        self.sandbox.create_snapshot("snap_v1")
        
        host_tmp, container_tmp = create_local_tmp_file("_APPEND")
        try:
            res_h_append, res_o_append = runner.run_dual_cmd("-appendToFile", container_tmp, f"{{TARGET}}{self.sandbox.test_dir}/rm_test.dat")
            assert res_h_append.returncode == 0, f"HDFS 追加失败: {res_h_append.stderr}"
            assert res_o_append.returncode == 0, f"OBSA 追加失败: {res_o_append.stderr}"
        finally:
            cleanup_local_tmp(host_tmp)
            
        runner.run_dual_cmd("-rm", f"{{TARGET}}{self.sandbox.test_dir}/rm_test.dat")
        
        res_h, res_o = runner.run_dual_cmd("-cat", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/rm_test.dat")
        self.validator.assert_results_match(res_h, res_o)
        assert res_h.stdout == "ORIG"