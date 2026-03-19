import pytest
import logging
from test_helpers import SnapshotSandbox, create_test_file

logger = logging.getLogger("TestSymlink")

class TestUnsupportedSymlink:
    """
    F17: 快照 × 拓扑异构拦截 (Symbolic Link)
    验证软链接操作在快照目录及快照路径下的拦截一致性。
    """

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner, validator):
        self.runner = runner
        self.validator = validator
        self.sandbox = SnapshotSandbox(runner, "f17_symlink_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    @pytest.mark.p2
    def test_f17_01_snapshot_with_symlink(self):
        """
        F17-01: 创建普通文件 -> 创建指向该文件的软链接 (hdfs dfs -ln) -> 对父目录打快照
        目标：验证 OBSA 是否在软链接创建阶段就进行了拦截。
        """
        target_file = f"{self.sandbox.test_dir}/real_file.dat"
        link_path = f"{self.sandbox.test_dir}/my_link"
        create_test_file(self.runner, target_file)
        
        # 创建软链接 (注意：ln 是 HDFS 较新版本的命令，有些老版本不支持，但 OBSA 肯定不支持)
        res_h, res_o = self.runner.run_dual_cmd("-ln", f"{{TARGET}}{target_file}", f"{{TARGET}}{link_path}")
        self.validator.assert_results_match(res_h, res_o, feature_tag="ln")
        
        # 即使创建失败或成功，我们尝试打快照看稳定性
        self.sandbox.create_snapshot("snap_symlink")
        
        # 如果 ln 成功，验证快照中的链接
        res_ls_h, res_ls_o = self.runner.run_dual_cmd("-ls", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_symlink")
        self.validator.assert_results_match(res_ls_h, res_ls_o, feature_tag="ls_snapshot_with_ln")
