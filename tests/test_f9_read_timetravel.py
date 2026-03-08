"""
F9: 快照 × 只读命令交互 (cat / get / tail / checksum / stat)
测试用例 F9-01 (P0)

验证从 .snapshot 路径读取数据的正确性，确保时间旅行语义。
"""
import pytest
import logging

from test_helpers import SnapshotSandbox, create_test_file, create_local_tmp_file, cleanup_local_tmp, create_test_file_with_size
from dual_runner import ParityValidator

logger = logging.getLogger("TestReadTimeTravel")


@pytest.mark.p0
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

    @pytest.mark.p2
    def test_f9_02_get_snapshot_file(self, runner):
        """F9-02: get 下载快照历史文件 (验证命令执行成功)"""
        content = "GET_TEST_F9_02"
        path = f"{self.sandbox.test_dir}/get_test.dat"
        create_test_file(runner, path, content)
        self.sandbox.create_snapshot("snap_v1")
        
        runner.run_dual_cmd("-rm", f"{{TARGET}}{path}")
        
        # 在 Docker 场景下直接验证本地文件不可靠，改用校验 cat 结果 + get 返回码
        local_dest = "/tmp/f9_02_downloaded.dat"
        res_h, res_o = runner.run_dual_cmd("-get", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/get_test.dat", local_dest)
        ParityValidator.assert_results_match(res_h, res_o)
        
        # 补充 cat 验证确保内容确实一致
        cat_h, _ = runner.run_dual_cmd("-cat", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/get_test.dat")
        assert cat_h.stdout == content

    @pytest.mark.p2
    def test_f9_03_checksum_time_travel(self, runner):
        """F9-03: checksum 验证快照时间旅行 (仅比对摘要部分)"""
        path = f"{self.sandbox.test_dir}/checksum_test.dat"
        create_test_file(runner, path, "SAME_DATA")
        
        res_h_pre, _ = runner.run_dual_cmd("-checksum", f"{{TARGET}}{path}")
        self.sandbox.create_snapshot("snap_v1")
        
        host_tmp, container_tmp = create_local_tmp_file("_MODIFIED")
        try:
            runner.run_dual_cmd("-appendToFile", container_tmp, f"{{TARGET}}{path}")
        finally:
            cleanup_local_tmp(host_tmp)
            
        res_h_snap, _ = runner.run_dual_cmd("-checksum", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/checksum_test.dat")
        
        # checksum 输出一般为: path MD5-of-0MD5-of-512CRC32 digest
        # 排除路径，仅对比摘要部分
        digest_pre = res_h_pre.stdout.split()[-1]
        digest_snap = res_h_snap.stdout.split()[-1]
        assert digest_pre == digest_snap

    @pytest.mark.p2
    def test_f9_04_stat_metadata_time_travel(self, runner):
        """F9-04: stat 元数据时间旅行"""
        path = f"{self.sandbox.test_dir}/stat_test.dat"
        create_test_file_with_size(runner, path, 1024)
        self.sandbox.create_snapshot("snap_v1")
        
        runner.run_dual_cmd("-truncate", "-w", "512", f"{{TARGET}}{path}")
        
        res_h, _ = runner.run_dual_cmd("-stat", "%b", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/stat_test.dat")
        # 期望块大小还是原来的量 (这里简化验证逻辑为 size 仍为 1024)
        assert "1024" in res_h.stdout
