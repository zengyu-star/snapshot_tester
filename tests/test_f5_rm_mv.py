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
    def test_f5_04_mv_rename_snapshot_preserves_old(self, runner):
        """F5-04: mv 重命名后，快照中仍保留旧文件名"""
        create_test_file(runner, f"{self.sandbox.test_dir}/old.dat")
        self.sandbox.create_snapshot("snap_v1")
        
        runner.run_dual_cmd("-mv", f"{{TARGET}}{self.sandbox.test_dir}/old.dat", f"{{TARGET}}{self.sandbox.test_dir}/new.dat")
        
        # 验证快照中旧文件名仍然存在
        res_h, res_o = runner.run_dual_cmd("-ls", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/old.dat")
        ParityValidator.assert_results_match(res_h, res_o)
        assert res_h.returncode == 0, "快照中应保留重命名前的旧文件"
        
        # 验证活跃目录中只有新文件名
        res_h2, _ = runner.run_dual_cmd("-ls", f"{{TARGET}}{self.sandbox.test_dir}/old.dat")
        assert res_h2.returncode != 0, "活跃目录中旧文件名应已不存在"

    @pytest.mark.p1
    def test_f5_05_mv_external_to_snapshot_dir(self, runner):
        """F5-05: 赋权 -> mv外部文件到快照目录 -> 快照A -> ls .snapshot/A -> 验证新文件在快照中可见"""
        external_dir = "/external_tmp_f5_05"
        runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{external_dir}")
        create_test_file(runner, f"{external_dir}/ext.dat", "EXTERNAL")
        
        # mv 入测试目录
        runner.run_dual_cmd("-mv", f"{{TARGET}}{external_dir}/ext.dat", f"{{TARGET}}{self.sandbox.test_dir}/ext.dat")
        
        # 创建快照
        self.sandbox.create_snapshot("snap_with_ext")
        
        # 验证可见
        res_h, res_o = runner.run_dual_cmd("-ls", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_with_ext/ext.dat")
        ParityValidator.assert_results_match(res_h, res_o)
        assert res_h.returncode == 0
        
        # 清理外部目录
        runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{external_dir}")

    @pytest.mark.p1
    def test_f5_06_rm_r_parent_containing_snapshot(self, runner):
        """F5-06: 赋权(子目录) -> 快照A -> 父目录 rm -r -> 拦截断言(不允许删除含快照的子目录)"""
        # 注意：HDFS 默认可能不允许嵌套的 snapshottable directory。
        # 故我们使用一个和 sandbox 根平行的测试目录，避免和 sandbox root (已 allowSnapshot) 冲突。
        parent_dir = "/f5_06_external_p"
        child_dir = f"{parent_dir}/c"
        
        # 清理旧数据并重新创建
        runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{parent_dir}")
        res, _ = runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{child_dir}")
        assert res.returncode == 0
        
        try:
            # 对子目录开启快照并创建
            res_allow, _ = runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{child_dir}")
            assert res_allow.returncode == 0, f"allowSnapshot failed: {res_allow.stderr}"
            
            res_create, _ = runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{child_dir}", "s1")
            assert res_create.returncode == 0, f"createSnapshot failed: {res_create.stderr}"
            
            # 尝试删除父目录
            res_h, res_o = runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{parent_dir}")
            
            # HDFS 应该拦截
            assert res_h.returncode != 0, "HDFS should block deleting parent of a snapshot"
            assert res_o.returncode != 0, "OBS should block deleting parent of a snapshot"
            ParityValidator.assert_results_match(res_h, res_o)
        finally:
            # 强制清理快照以便后续删除
            runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{child_dir}", "s1")
            runner.run_dual_admin_cmd("-disallowSnapshot", f"{{TARGET}}{child_dir}")
            runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{parent_dir}")
