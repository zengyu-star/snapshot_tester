import pytest
import logging
import os
import sys

# 添加项目根目录到 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_helpers import SnapshotSandbox, create_test_file


logger = logging.getLogger("TestQueryInteractions")

@pytest.mark.p2
class TestQueryInteractions:
    """F10: 快照 × 信息查询交互 (ls / count / du)"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner, validator):
        self.runner = runner
        self.validator = validator
        self.sandbox = SnapshotSandbox(runner, "f10_query_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    def test_f10_01_ls_snapshot_consistency(self, runner):
        """F10-01: 快照路径 ls 反映历史状态"""
        create_test_file(runner, f"{self.sandbox.test_dir}/file1.txt")
        create_test_file(runner, f"{self.sandbox.test_dir}/file2.txt")
        self.sandbox.create_snapshot("snap_v1")
        
        runner.run_dual_cmd("-rm", f"{{TARGET}}{self.sandbox.test_dir}/file1.txt")
        
        res_h, res_o = runner.run_dual_cmd("-ls", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1")
        self.validator.assert_results_match(res_h, res_o)
        assert "file1.txt" in res_h.stdout
        assert "file2.txt" in res_h.stdout

    def test_f10_02_count_isolation(self, runner):
        """F10-02: count 计数时间旅行"""
        create_test_file(runner, f"{self.sandbox.test_dir}/f1")
        res_h_pre, _ = runner.run_dual_cmd("-count", f"{{TARGET}}{self.sandbox.test_dir}")
        self.sandbox.create_snapshot("snap_v1")
        
        runner.run_dual_cmd("-rm", f"{{TARGET}}{self.sandbox.test_dir}/f1")
        
        res_h_post, _ = runner.run_dual_cmd("-count", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1")
        # count 输出格式: DIR_COUNT FILE_COUNT CONTENT_SIZE PATH
        # 简化验证：历史快照的内容和路径不应受删除影响
        assert res_h_post.stdout.split()[:-1] == res_h_pre.stdout.split()[:-1]

    def test_f10_03_du_isolation(self, runner):
        """F10-03: du 空间统计时间旅行"""
        if not runner.mock_mode:
            # 只有在非 Mock 模式（即真实挂载 OBSA 的实测模式）下，才将其标记为预期失败
            pytest.xfail(reason="根据设计文档，不支持快照的文件的列举")
        create_test_file(runner, f"{self.sandbox.test_dir}/size_file", "X" * 100)
        self.sandbox.create_snapshot("snap_v1")
        
        runner.run_dual_cmd("-rm", f"{{TARGET}}{self.sandbox.test_dir}/size_file")
        
        res_h, res_o = runner.run_dual_cmd("-du", "-s", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1")
        self.validator.assert_results_match(res_h, res_o)
        assert "100" in res_h.stdout

    def test_f10_04_ls_all_snapshots(self, runner):
        """F10-04: ls .snapshot 列出全部快照"""
        self.sandbox.create_snapshot("S1")
        self.sandbox.create_snapshot("S2")
        
        res_h, res_o = runner.run_dual_cmd("-ls", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot")
        self.validator.assert_results_match(res_h, res_o)
        assert "S1" in res_h.stdout
        assert "S2" in res_h.stdout
