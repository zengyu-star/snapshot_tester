"""
F11: 只读隔离性 —— .snapshot 路径写操作全拦截
测试用例 F11-01 ~ F11-08 (P0)

对 .snapshot/snapName/ 下的每种写入类命令逐一验证拦截。
双端（HDFS + OBS/Mock）必须同时拦截并返回一致的错误。
"""
import pytest
import logging
import os

from test_helpers import SnapshotSandbox, create_test_file, create_local_tmp_file, cleanup_local_tmp
from dual_runner import ParityValidator

logger = logging.getLogger("TestReadonlyBlock")


@pytest.mark.p0
class TestReadonlyIsolation:
    """只读隔离性 —— .snapshot 写操作全拦截 (F11-01 ~ F11-08)"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner):
        self.runner = runner
        self.sandbox = SnapshotSandbox(runner, "f11_readonly_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        # 预先创建一些文件以供测试
        create_test_file(runner, f"{self.sandbox.test_dir}/existing_file.dat", "READONLY_TEST")
        self.sandbox.create_snapshot("snap_v1")
        self.snap_path = f"{self.sandbox.test_dir}/.snapshot/snap_v1"
        yield
        self.sandbox.teardown()

    def _assert_both_blocked(self, res_h, res_o, op_name):
        """断言双端均拦截（返回非 0 状态码）且行为一致"""
        assert res_h.returncode != 0, \
            f"致命漏洞: HDFS 允许了对快照路径的 {op_name} 操作! stdout={res_h.stdout} stderr={res_h.stderr}"
        assert res_o.returncode != 0, \
            f"致命漏洞: OBS/Mock 允许了对快照路径的 {op_name} 操作! stdout={res_o.stdout} stderr={res_o.stderr}"
        # 双端状态码一致
        assert res_h.returncode == res_o.returncode or (res_h.returncode != 0 and res_o.returncode != 0), \
            f"{op_name}: 双端均拦截但状态码不同 HDFS={res_h.returncode} OBS={res_o.returncode}"

    def test_f11_01_touchz_blocked(self, runner):
        """F11-01: touchz .snapshot/A/new_file -> 拦截断言"""
        res_h, res_o = runner.run_dual_cmd(
            "-touchz", f"{{TARGET}}{self.snap_path}/illegal_touchz.dat"
        )
        self._assert_both_blocked(res_h, res_o, "touchz")

    def test_f11_02_put_blocked(self, runner):
        """F11-02: put local_file .snapshot/A/ -> 拦截断言"""
        host_tmp, container_tmp = create_local_tmp_file("ILLEGAL_PUT")
        try:
            res_h, res_o = runner.run_dual_cmd(
                "-put", container_tmp, f"{{TARGET}}{self.snap_path}/illegal_put.dat"
            )
        finally:
            cleanup_local_tmp(host_tmp)
        self._assert_both_blocked(res_h, res_o, "put")

    def test_f11_03_append_blocked(self, runner):
        """F11-03: appendToFile .snapshot/A/file -> 拦截断言"""
        host_tmp, container_tmp = create_local_tmp_file("ILLEGAL_APPEND")
        try:
            res_h, res_o = runner.run_dual_cmd(
                "-appendToFile", container_tmp,
                f"{{TARGET}}{self.snap_path}/existing_file.dat"
            )
        finally:
            cleanup_local_tmp(host_tmp)
        self._assert_both_blocked(res_h, res_o, "appendToFile")

    def test_f11_04_rm_blocked(self, runner):
        """F11-04: rm .snapshot/A/file -> 拦截断言"""
        res_h, res_o = runner.run_dual_cmd(
            "-rm", f"{{TARGET}}{self.snap_path}/existing_file.dat"
        )
        self._assert_both_blocked(res_h, res_o, "rm")

    def test_f11_05_mv_source_blocked(self, runner):
        """F11-05: mv .snapshot/A/file newpath -> 拦截断言 (源端在快照)"""
        res_h, res_o = runner.run_dual_cmd(
            "-mv",
            f"{{TARGET}}{self.snap_path}/existing_file.dat",
            f"{{TARGET}}{self.sandbox.test_dir}/stolen_file.dat"
        )
        self._assert_both_blocked(res_h, res_o, "mv(source)")

    def test_f11_06_mv_target_blocked(self, runner):
        """F11-06: mv somefile .snapshot/A/ -> 拦截断言 (目标端在快照)"""
        # 先创建一个活跃目录中的文件作为 mv 源
        create_test_file(runner, f"{self.sandbox.test_dir}/mv_source.dat", "MV_SOURCE")
        res_h, res_o = runner.run_dual_cmd(
            "-mv",
            f"{{TARGET}}{self.sandbox.test_dir}/mv_source.dat",
            f"{{TARGET}}{self.snap_path}/mv_source.dat"
        )
        self._assert_both_blocked(res_h, res_o, "mv(target)")

    def test_f11_07_mkdir_blocked(self, runner):
        """F11-07: mkdir .snapshot/A/newdir -> 拦截断言"""
        res_h, res_o = runner.run_dual_cmd(
            "-mkdir", f"{{TARGET}}{self.snap_path}/illegal_dir"
        )
        self._assert_both_blocked(res_h, res_o, "mkdir")

    def test_f11_08_rm_r_snapshot_root_blocked(self, runner):
        """F11-08: rm -r .snapshot/A -> 拦截断言 (必须通过 deleteSnapshot 删除)"""
        res_h, res_o = runner.run_dual_cmd(
            "-rm", "-r", f"{{TARGET}}{self.snap_path}"
        )
        self._assert_both_blocked(res_h, res_o, "rm -r snapshot_root")
