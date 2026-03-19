import pytest
import logging
import os
import sys

# 添加项目根目录到 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_helpers import SnapshotSandbox, create_test_file, create_test_file_with_size


logger = logging.getLogger("TestTruncateInteraction")

class TestTruncateInteraction:
    """快照 × truncate 命令交互 (F4-01 ~ F4-08)"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner, validator):
        self.runner = runner
        self.validator = validator
        self.sandbox = SnapshotSandbox(runner, "f4_truncate_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    @pytest.mark.p1
    def test_f4_01_truncate_smaller_preserves_snapshot(self, runner):
        """F4-01: 赋权 -> 造数(1KB) -> 快照A -> truncate(512B) -> 验证快照中文件大小不变"""
        create_test_file_with_size(runner, f"{self.sandbox.test_dir}/trunc_small.dat", 1024)
        self.sandbox.create_snapshot("snap_v1")

        runner.run_dual_cmd("-truncate", "-w", "512", f"{{TARGET}}{self.sandbox.test_dir}/trunc_small.dat")

        # 验证快照中文件大小仍为原始 1024 字节
        res_h, res_o = runner.run_dual_cmd("-du", "-s", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/trunc_small.dat")
        self.validator.assert_results_match(res_h, res_o)
        assert "1024" in res_h.stdout, f"快照中的文件应保持原始大小，实际: {res_h.stdout}"

    @pytest.mark.p1
    def test_f4_02_truncate_zero_preserves_snapshot(self, runner):
        """F4-02: 赋权 -> 造数 -> 快照A -> truncate(0) -> cat .snapshot/A/file -> 验证完整性"""
        content = "TRUNCATE_TEST_DATA"
        create_test_file(runner, f"{self.sandbox.test_dir}/trunc_zero.dat", content)
        self.sandbox.create_snapshot("snap_v1")

        runner.run_dual_cmd("-truncate", "-w", "0", f"{{TARGET}}{self.sandbox.test_dir}/trunc_zero.dat")

        res_h, res_o = runner.run_dual_cmd("-cat", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/trunc_zero.dat")
        self.validator.assert_results_match(res_h, res_o)
        assert res_h.stdout == content

    @pytest.mark.p1
    def test_f4_04_truncate_snapshot_path_forbidden(self, runner):
        """F4-04: 赋权 -> 造数 -> 快照A -> truncate .snapshot/A/file -> 拦截断言"""
        create_test_file(runner, f"{self.sandbox.test_dir}/no_touch.dat")
        self.sandbox.create_snapshot("snap_v1")

        res_h, res_o = runner.run_dual_cmd("-truncate", "-w", "0", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/no_touch.dat")
        assert res_h.returncode != 0
        assert res_o.returncode != 0
        self.validator.assert_results_match(res_h, res_o)

    @pytest.mark.p1
    def test_f4_05_truncate_larger_parity(self, runner):
        """F4-05: 赋权 -> 造数(512B) -> 快照A -> truncate(1KB) -> 双端一致性比对"""
        # HDFS 原生并不支持截大，通常会报错或行为一致。
        create_test_file_with_size(runner, f"{self.sandbox.test_dir}/trunc_large.dat", 512)
        self.sandbox.create_snapshot("snap_v1")

        res_h, res_o = runner.run_dual_cmd("-truncate", "-w", "1024", f"{{TARGET}}{self.sandbox.test_dir}/trunc_large.dat")
        self.validator.assert_results_match(res_h, res_o)

    @pytest.mark.p1
    def test_f4_06_truncate_larger_snapshot_content_preserved(self, runner):
        """F4-06: truncate 截大后快照中文件内容不受影响"""
        create_test_file_with_size(runner, f"{self.sandbox.test_dir}/content_iso.dat", 512)
        self.sandbox.create_snapshot("snap_v1")
        
        # 尝试截大 (通常 HDFS 会失败，如果成功则验证隔离性)
        runner.run_dual_cmd("-truncate", "-w", "1024", f"{{TARGET}}{self.sandbox.test_dir}/content_iso.dat")
        
        # 验证快照中依然是 512B
        res_h, _ = runner.run_dual_cmd("-stat", "%b", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/content_iso.dat")
        assert "512" in res_h.stdout
