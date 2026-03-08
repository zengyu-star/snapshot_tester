"""
F2: snapshotDiff 差异标记全覆盖
测试用例 F2-01 ~ F2-09

验证 snapshotDiff 报告中 +/-/M/R 每种标记的准确性（双端一致）。
"""
import pytest
import logging
import os

from test_helpers import SnapshotSandbox, create_test_file, create_test_file_with_size, create_local_tmp_file, cleanup_local_tmp
from dual_runner import ParityValidator

logger = logging.getLogger("TestSnapshotDiff")


class TestSnapshotDiff:
    """snapshotDiff 差异标记全覆盖 (F2-01 ~ F2-09)"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner):
        self.runner = runner
        self.sandbox = SnapshotSandbox(runner, "f2_diff_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    def test_f2_01_diff_add_file(self, runner):
        """F2-01: 赋权 -> 快照A -> 新增文件 -> 快照B -> snapshotDiff -> 验证"+"标记"""
        self.sandbox.create_snapshot("snap_v1")
        create_test_file(runner, f"{self.sandbox.test_dir}/new_file.dat")
        self.sandbox.create_snapshot("snap_v2")

        res_h, res_o = runner.run_dual_hdfs_cmd(
            "snapshotDiff", f"{{TARGET}}{self.sandbox.test_dir}", "snap_v1", "snap_v2"
        )
        ParityValidator.assert_results_match(res_h, res_o)
        assert "+" in res_h.stdout, f"HDFS diff 中未发现 '+' 标记: {res_h.stdout}"
        assert "+" in res_o.stdout, f"OBS diff 中未发现 '+' 标记: {res_o.stdout}"

    def test_f2_02_diff_delete_file(self, runner):
        """F2-02: 赋权 -> 造数 -> 快照A -> rm文件 -> 快照B -> snapshotDiff -> 验证"-"标记"""
        create_test_file(runner, f"{self.sandbox.test_dir}/to_delete.dat")
        self.sandbox.create_snapshot("snap_v1")

        runner.run_dual_cmd("-rm", f"{{TARGET}}{self.sandbox.test_dir}/to_delete.dat")
        self.sandbox.create_snapshot("snap_v2")

        res_h, res_o = runner.run_dual_hdfs_cmd(
            "snapshotDiff", f"{{TARGET}}{self.sandbox.test_dir}", "snap_v1", "snap_v2"
        )
        ParityValidator.assert_results_match(res_h, res_o)
        assert "-" in res_h.stdout, f"HDFS diff 中未发现 '-' 标记: {res_h.stdout}"

    def test_f2_03_diff_modify_append(self, runner):
        """F2-03: 赋权 -> 造数 -> 快照A -> appendToFile -> 快照B -> snapshotDiff -> 验证"M"标记"""
        create_test_file(runner, f"{self.sandbox.test_dir}/to_modify.dat", "ORIGINAL")
        self.sandbox.create_snapshot("snap_v1")

        host_tmp, container_tmp = create_local_tmp_file("APPENDED")
        try:
            runner.run_dual_cmd("-appendToFile", container_tmp, f"{{TARGET}}{self.sandbox.test_dir}/to_modify.dat")
        finally:
            cleanup_local_tmp(host_tmp)

        self.sandbox.create_snapshot("snap_v2")

        res_h, res_o = runner.run_dual_hdfs_cmd(
            "snapshotDiff", f"{{TARGET}}{self.sandbox.test_dir}", "snap_v1", "snap_v2"
        )
        ParityValidator.assert_results_match(res_h, res_o)
        assert "M" in res_h.stdout, f"HDFS diff 中未发现 'M' 标记: {res_h.stdout}"

    def test_f2_04_diff_rename_file(self, runner):
        """F2-04: 赋权 -> 造数 -> 快照A -> mv文件(目录内重命名) -> 快照B -> snapshotDiff -> 验证"R"标记"""
        create_test_file(runner, f"{self.sandbox.test_dir}/old_name.dat")
        self.sandbox.create_snapshot("snap_v1")

        runner.run_dual_cmd(
            "-mv",
            f"{{TARGET}}{self.sandbox.test_dir}/old_name.dat",
            f"{{TARGET}}{self.sandbox.test_dir}/new_name.dat"
        )
        self.sandbox.create_snapshot("snap_v2")

        res_h, res_o = runner.run_dual_hdfs_cmd(
            "snapshotDiff", f"{{TARGET}}{self.sandbox.test_dir}", "snap_v1", "snap_v2"
        )
        ParityValidator.assert_results_match(res_h, res_o)
        # HDFS rename 在 snapshotDiff 中可能表现为 "R" 或 "+/-" 组合
        has_rename = "R" in res_h.stdout or ("+" in res_h.stdout and "-" in res_h.stdout)
        assert has_rename, f"HDFS diff 中未发现重命名标记: {res_h.stdout}"

    def test_f2_05_diff_add_directory(self, runner):
        """F2-05: 赋权 -> 快照A -> mkdir新子目录 -> 快照B -> snapshotDiff -> 验证"+目录"标记"""
        self.sandbox.create_snapshot("snap_v1")

        runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{self.sandbox.test_dir}/new_subdir")
        self.sandbox.create_snapshot("snap_v2")

        res_h, res_o = runner.run_dual_hdfs_cmd(
            "snapshotDiff", f"{{TARGET}}{self.sandbox.test_dir}", "snap_v1", "snap_v2"
        )
        ParityValidator.assert_results_match(res_h, res_o)
        assert "+" in res_h.stdout, f"HDFS diff 中未发现 '+' 标记: {res_h.stdout}"

    def test_f2_06_diff_delete_directory(self, runner):
        """F2-06: 赋权 -> 造数(含子目录) -> 快照A -> rm -r子目录 -> 快照B -> snapshotDiff -> 验证"-目录"标记"""
        runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{self.sandbox.test_dir}/sub_to_del")
        create_test_file(runner, f"{self.sandbox.test_dir}/sub_to_del/file.dat")
        self.sandbox.create_snapshot("snap_v1")

        runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{self.sandbox.test_dir}/sub_to_del")
        self.sandbox.create_snapshot("snap_v2")

        res_h, res_o = runner.run_dual_hdfs_cmd(
            "snapshotDiff", f"{{TARGET}}{self.sandbox.test_dir}", "snap_v1", "snap_v2"
        )
        ParityValidator.assert_results_match(res_h, res_o)
        assert "-" in res_h.stdout, f"HDFS diff 中未发现 '-' 标记: {res_h.stdout}"

    def test_f2_07_diff_mixed_mutations(self, runner):
        """F2-07: 赋权 -> 造数 -> 快照A -> 混合变异(增+删+改) -> 快照B -> snapshotDiff -> 验证多标记共存"""
        create_test_file(runner, f"{self.sandbox.test_dir}/keep_file.dat", "KEEP")
        create_test_file(runner, f"{self.sandbox.test_dir}/del_file.dat", "DELETE_ME")
        create_test_file(runner, f"{self.sandbox.test_dir}/mod_file.dat", "MODIFY_ME")
        self.sandbox.create_snapshot("snap_v1")

        # 新增
        create_test_file(runner, f"{self.sandbox.test_dir}/added_file.dat", "ADDED")
        # 删除
        runner.run_dual_cmd("-rm", f"{{TARGET}}{self.sandbox.test_dir}/del_file.dat")
        # 修改(追加)
        host_tmp, container_tmp = create_local_tmp_file("_APPENDED")
        try:
            runner.run_dual_cmd("-appendToFile", container_tmp, f"{{TARGET}}{self.sandbox.test_dir}/mod_file.dat")
        finally:
            cleanup_local_tmp(host_tmp)

        self.sandbox.create_snapshot("snap_v2")

        res_h, res_o = runner.run_dual_hdfs_cmd(
            "snapshotDiff", f"{{TARGET}}{self.sandbox.test_dir}", "snap_v1", "snap_v2"
        )
        ParityValidator.assert_results_match(res_h, res_o)
        assert "+" in res_h.stdout, f"diff 中缺少 '+' 标记"
        assert "-" in res_h.stdout, f"diff 中缺少 '-' 标记"
        assert "M" in res_h.stdout, f"diff 中缺少 'M' 标记"

    def test_f2_08_diff_no_change(self, runner):
        """F2-08: 赋权 -> 造数 -> 快照A -> 无变异 -> 快照B -> snapshotDiff -> 验证diff为空"""
        create_test_file(runner, f"{self.sandbox.test_dir}/stable.dat")
        self.sandbox.create_snapshot("snap_v1")
        self.sandbox.create_snapshot("snap_v2")

        res_h, res_o = runner.run_dual_hdfs_cmd(
            "snapshotDiff", f"{{TARGET}}{self.sandbox.test_dir}", "snap_v1", "snap_v2"
        )
        ParityValidator.assert_results_match(res_h, res_o)
        # diff 输出应只有 header 行，不包含 +/-/M/R 标记
        for marker in ["+\t", "-\t", "M\t", "R\t"]:
            assert marker not in res_h.stdout, f"无变异但 diff 中出现了 '{marker.strip()}' 标记"

    def test_f2_09_diff_truncate(self, runner):
        """F2-09: 赋权 -> 造数 -> 快照A -> truncate文件 -> 快照B -> snapshotDiff -> 验证"M"标记"""
        create_test_file_with_size(runner, f"{self.sandbox.test_dir}/trunc_file.dat", 1024)
        self.sandbox.create_snapshot("snap_v1")

        runner.run_dual_cmd("-truncate", "-w", "512", f"{{TARGET}}{self.sandbox.test_dir}/trunc_file.dat")
        self.sandbox.create_snapshot("snap_v2")

        res_h, res_o = runner.run_dual_hdfs_cmd(
            "snapshotDiff", f"{{TARGET}}{self.sandbox.test_dir}", "snap_v1", "snap_v2"
        )
        ParityValidator.assert_results_match(res_h, res_o)
        assert "M" in res_h.stdout, f"truncate 后 diff 中未发现 'M' 标记: {res_h.stdout}"
