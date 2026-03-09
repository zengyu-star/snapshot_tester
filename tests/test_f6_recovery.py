import pytest
import logging
import os
import sys

# 添加项目根目录到 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_helpers import SnapshotSandbox, create_test_file
from dual_runner import ParityValidator

logger = logging.getLogger("TestRecoveryCP")

@pytest.mark.p2
class TestRecoveryCP:
    """F6: 快照 × cp 命令交互（含快照恢复）"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner):
        self.runner = runner
        self.sandbox = SnapshotSandbox(runner, "f6_recovery_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    def test_f6_01_recovery_single_file(self, runner):
        """F6-01: 从快照路径 cp 恢复单文件控制流"""
        content = "RECOVERY_DATA_F6_01"
        create_test_file(runner, f"{self.sandbox.test_dir}/file.dat", content)
        self.sandbox.create_snapshot("snap_v1")
        
        runner.run_dual_cmd("-rm", f"{{TARGET}}{self.sandbox.test_dir}/file.dat")
        
        # 恢复
        res_h, res_o = runner.run_dual_cmd("-cp", 
            f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/file.dat", 
            f"{{TARGET}}{self.sandbox.test_dir}/file_recovered.dat"
        )
        ParityValidator.assert_results_match(res_h, res_o)
        
        # 校验恢复后的内容
        ls_h, _ = runner.run_dual_cmd("-cat", f"{{TARGET}}{self.sandbox.test_dir}/file_recovered.dat")
        assert ls_h.stdout == content

    def test_f6_02_recovery_directory_tree(self, runner):
        """F6-02: 从快照路径 cp 恢复整个子目录树"""
        runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{self.sandbox.test_dir}/sub/inner")
        create_test_file(runner, f"{self.sandbox.test_dir}/sub/inner/f1.txt", "TREE_DATA")
        self.sandbox.create_snapshot("snap_tree")
        
        runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{self.sandbox.test_dir}/sub")
        
        # 恢复整个目录
        res_h, res_o = runner.run_dual_cmd("-cp", "-r",
            f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_tree/sub",
            f"{{TARGET}}{self.sandbox.test_dir}/sub_recovered"
        )
        ParityValidator.assert_results_match(res_h, res_o)
        
        # 验证恢复成功
        res_ls, _ = runner.run_dual_cmd("-ls", f"{{TARGET}}{self.sandbox.test_dir}/sub_recovered/inner/f1.txt")
        assert res_ls.returncode == 0

    def test_f6_03_recovery_content_checksum(self, runner):
        """F6-03: 恢复文件的内容校验 (checksum)"""
        path = f"{self.sandbox.test_dir}/check.dat"
        create_test_file(runner, path, "CHECKSUM_STABILITY_TEST")
        pre_h, _ = runner.run_dual_cmd("-checksum", f"{{TARGET}}{path}")
        self.sandbox.create_snapshot("s1")
        
        # 变异并恢复
        runner.run_dual_cmd("-rm", f"{{TARGET}}{path}")
        runner.run_dual_cmd("-cp", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/s1/check.dat", f"{{TARGET}}{path}")
        
        post_h, _ = runner.run_dual_cmd("-checksum", f"{{TARGET}}{path}")
        # 校验摘要一致
        assert pre_h.stdout.split()[-1] == post_h.stdout.split()[-1]

    def test_f6_04_cp_to_snapshot_forbidden(self, runner):
        """F6-04: 禁止向 .snapshot 路径 cp 写入"""
        create_test_file(runner, f"{self.sandbox.test_dir}/src.dat")
        self.sandbox.create_snapshot("snap_v1")
        
        res_h, res_o = runner.run_dual_cmd("-cp", 
            f"{{TARGET}}{self.sandbox.test_dir}/src.dat", 
            f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_v1/target.dat"
        )
        assert res_h.returncode != 0
        assert res_o.returncode != 0
        ParityValidator.assert_results_match(res_h, res_o)

    def test_f6_05_cp_from_snapshot_to_outside(self, runner):
        """F6-05: 从快照拷贝到外部路径"""
        create_test_file(runner, f"{self.sandbox.test_dir}/out.dat", "OUTSIDE")
        self.sandbox.create_snapshot("s1")
        
        external_path = "/tmp/external_recovery_f6_05.dat"
        res_h, res_o = runner.run_dual_cmd("-cp",
            f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/s1/out.dat",
            f"{{TARGET}}{external_path}"
        )
        ParityValidator.assert_results_match(res_h, res_o)
        
        # 验证外部文件
        cat_h, _ = runner.run_dual_cmd("-cat", f"{{TARGET}}{external_path}")
        assert cat_h.stdout == "OUTSIDE"
        # 清理
        runner.run_dual_cmd("-rm", f"{{TARGET}}{external_path}")
