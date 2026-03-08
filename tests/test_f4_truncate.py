import pytest
import logging
import os
import sys

# 添加项目根目录到 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_helpers import SnapshotSandbox, create_test_file, create_test_file_with_size
from dual_runner import ParityValidator

logger = logging.getLogger("TestTruncateInteraction")

class TestTruncateInteraction:
    """快照 × truncate 命令交互 (F4-01 ~ F4-08)"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner):
        self.runner = runner
        self.sandbox = SnapshotSandbox(runner, "f4_truncate_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    @pytest.mark.p1
    def test_f4_01_truncate_smaller_diff(self, runner):
        """F4-01: 赋权 -> 造数(1KB) -> 快照A -> truncate(512B) -> 快照B -> snapshotDiff -> 验证M标记"""
        create_test_file_with_size(runner, f"{self.sandbox.test_dir}/trunc_small.dat", 1024)
        self.sandbox.create_snapshot("snap_v1")

        runner.run_dual_cmd("-truncate", "-w", "512", f"{{TARGET}}{self.sandbox.test_dir}/trunc_small.dat")
        self.sandbox.create_snapshot("snap_v2")

        res_h, res_o = runner.run_dual_hdfs_cmd("snapshotDiff", f"{{TARGET}}{self.sandbox.test_dir}", "snap_v1", "snap_v2")
        ParityValidator.assert_results_match(res_h, res_o)
        assert "M" in res_h.stdout

    @pytest.mark.p1
    def test_f4_02_truncate_zero_preserves_snapshot(self, runner):
        """F4-02: 赋权 -> 造数 -> 快照A -> truncate(0) -> cat .snapshot/A/file -> 验证完整性"""
        content = "TRUNCATE_TEST_DATA"
        create_test_file(runner, f"{self.sandbox.test_dir}/trunc_zero.dat", content)
        self.sandbox.create_snapshot("snap_v1")

        runner.run_dual_cmd("-truncate", "-w", "0", f"{{TARGET}}{self.sandbox.test_dir}/trunc_zero.dat")

        res_h, res_o = runner.run_dual_cmd("-cat", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/trunc_zero.dat")
        ParityValidator.assert_results_match(res_h, res_o)
        assert res_h.stdout == content

    @pytest.mark.p1
    def test_f4_04_truncate_snapshot_path_forbidden(self, runner):
        """F4-04: 赋权 -> 造数 -> 快照A -> truncate .snapshot/A/file -> 拦截断言"""
        create_test_file(runner, f"{self.sandbox.test_dir}/no_touch.dat")
        self.sandbox.create_snapshot("snap_v1")

        res_h, res_o = runner.run_dual_cmd("-truncate", "-w", "0", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/no_touch.dat")
        assert res_h.returncode != 0
        assert res_o.returncode != 0
        ParityValidator.assert_results_match(res_h, res_o)

    @pytest.mark.p1
    def test_f4_05_truncate_larger_parity(self, runner):
        """F4-05: 赋权 -> 造数(512B) -> 快照A -> truncate(1KB) -> 双端一致性比对"""
        # HDFS 原生并不支持截大，通常会报错或行为一致。
        create_test_file_with_size(runner, f"{self.sandbox.test_dir}/trunc_large.dat", 512)
        self.sandbox.create_snapshot("snap_v1")

        res_h, res_o = runner.run_dual_cmd("-truncate", "-w", "1024", f"{{TARGET}}{self.sandbox.test_dir}/trunc_large.dat")
        ParityValidator.assert_results_match(res_h, res_o)
