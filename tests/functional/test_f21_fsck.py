import pytest
import logging
from test_helpers import SnapshotSandbox, create_test_file

logger = logging.getLogger("TestFsck")

class TestDiagnosticTools:
    """
    F21: 快照 × 诊断工具交互 (fsck)
    验证 HDFS 管理员工具在快照路径下的扫描兼容性。
    """

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner, validator):
        self.runner = runner
        self.validator = validator
        self.sandbox = SnapshotSandbox(runner, "f21_fsck_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    @pytest.mark.p2
    def test_f21_01_fsck_snapshot_path(self):
        """
        F21-01: 造数 -> 快照A -> hdfs fsck .snapshot/A
        目标：验证 fsck 对快照路径的支持。
        OBSA 可能会拦截 fsck 对快照路径的查询，或者返回成功但空结果。
        """
        create_test_file(self.runner, f"{self.sandbox.test_dir}/health_check.dat", "HEALTHY_DATA")
        self.sandbox.create_snapshot("snap_for_fsck")
        
        # 执行 fsck
        res_h, res_o = self.runner.run_dual_hdfs_cmd("fsck", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_for_fsck")
        
        # 验证结果匹配或拦截
        self.validator.assert_results_match(res_h, res_o, feature_tag="fsck")
