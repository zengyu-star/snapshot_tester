import pytest
import logging
from test_helpers import SnapshotSandbox, create_test_file

logger = logging.getLogger("TestQuota")

class TestUnsupportedQuota:
    """
    F18: 快照 × 容量配额交互 (Quota)
    验证不支持的 Quota 操作在快照场景下的拦截一致性。
    """

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner, validator):
        self.runner = runner
        self.validator = validator
        self.sandbox = SnapshotSandbox(runner, "f18_quota_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    @pytest.mark.p2
    def test_f18_01_snapshot_name_quota_consumption(self):
        """
        F18-01: 设置 NameQuota=5 -> 造数3个 -> 快照 -> 再造3个文件
        目标：验证 NameQuota 拦截逻辑。
        """
        # 设置 NameQuota
        res_h, res_o = self.runner.run_dual_admin_cmd("-setQuota", "5", f"{{TARGET}}{self.sandbox.test_dir}")
        self.validator.assert_results_match(res_h, res_o, feature_tag="setQuota")
        
        # 造数 3 个
        for i in range(3):
            create_test_file(self.runner, f"{self.sandbox.test_dir}/file_{i}.dat")
            
        # 打快照 (消耗 1 个元数据配额)
        self.sandbox.create_snapshot("snap_quota_1")
        
        # 再造数，验证是否触发异常
        res_h2, res_o2 = self.runner.run_dual_cmd("-touchz", f"{{TARGET}}{self.sandbox.test_dir}/file_after_quota.dat")
        self.validator.assert_results_match(res_h2, res_o2, feature_tag="quota_enforcement")

    @pytest.mark.p2
    def test_f18_02_snapshot_space_quota_consumption(self):
        """
        F18-02: 设置 SpaceQuota -> 造大文件 -> 快照 -> append 变异文件
        目标：验证 SpaceQuota 拦截逻辑。
        """
        # 设置 SpaceQuota (10GB, 确保在 Mock 模式下不因环境残留数据触发过载)
        res_h, res_o = self.runner.run_dual_admin_cmd("-setSpaceQuota", "10G", f"{{TARGET}}{self.sandbox.test_dir}")
        self.validator.assert_results_match(res_h, res_o, feature_tag="setSpaceQuota")
        
        # 造一个小文件，验证配额内可成功
        test_file = f"{self.sandbox.test_dir}/quota_file.dat"
        create_test_file(self.runner, test_file, content="SURE_PASS")
        
        self.sandbox.create_snapshot("snap_space_quota")
        
        # 清除 Quota (为了后续清理)
        self.runner.run_dual_admin_cmd("-clrSpaceQuota", f"{{TARGET}}{self.sandbox.test_dir}")
        self.runner.run_dual_admin_cmd("-clrQuota", f"{{TARGET}}{self.sandbox.test_dir}")
