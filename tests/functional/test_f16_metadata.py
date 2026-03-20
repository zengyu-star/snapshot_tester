import pytest
import logging
from test_helpers import SnapshotSandbox, create_test_file

logger = logging.getLogger("TestMetadata")

class TestUnsupportedMetadata:
    """
    F16: 快照 × 高级元数据隔离 (XAttr / ACL / Storage policy)
    验证不支持的元数据操作在快照场景下的拦截一致性。
    """

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner, validator):
        self.runner = runner
        self.validator = validator
        self.sandbox = SnapshotSandbox(runner, "f16_metadata_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    @pytest.mark.p1
    def test_f16_01_setfattr_interaction(self):
        """
        F16-01: 造数 -> setfattr 设置扩展属性 -> 快照 -> getfattr 读取
        目标：验证 XAttr 拦截逻辑。
        """
        test_file = f"{self.sandbox.test_dir}/xattr_file.dat"
        create_test_file(self.runner, test_file)
        
        # 设置 XAttr
        res_h, res_o = self.runner.run_dual_cmd("-setfattr", "-n", "user.test_attr", "-v", "test_val", f"{{TARGET}}{test_file}")
        self.validator.assert_results_match(res_h, res_o, feature_tag="setfattr")
        
        self.sandbox.create_snapshot("snap_xattr")
        
        # 读取 XAttr
        res_h, res_o = self.runner.run_dual_cmd("-getfattr", "-n", "user.test_attr", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_xattr/xattr_file.dat")
        self.validator.assert_results_match(res_h, res_o, feature_tag="getfattr")

    @pytest.mark.p1
    def test_f16_02_setfacl_interaction(self):
        """
        F16-02: 造数 -> setfacl 设置访问控制列表 -> 快照 -> getfacl 读取
        目标：验证 ACL 拦截逻辑。
        """
        # 【修复点】：增加 self. 前缀，调用类属性
        if not self.runner.mock_mode:
            # 只有在非 Mock 模式（即真实挂载 OBSA 的实测模式）下，才将其标记为预期失败
            pytest.xfail(reason="官方文档，OBSA挂载场景下，不支持Posix ACL")
        test_file = f"{self.sandbox.test_dir}/acl_file.dat"
        create_test_file(self.runner, test_file)
        
        # 设置 ACL
        res_h, res_o = self.runner.run_dual_cmd("-setfacl", "-m", "user:nobody:rwx", f"{{TARGET}}{test_file}")
        self.validator.assert_results_match(res_h, res_o, feature_tag="setfacl")
        
        self.sandbox.create_snapshot("snap_acl")
        
        # 读取 ACL
        res_h, res_o = self.runner.run_dual_cmd("-getfacl", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_acl/acl_file.dat")
        self.validator.assert_results_match(res_h, res_o, feature_tag="getfacl")

    @pytest.mark.p2
    def test_f16_03_storage_policy_interaction(self):
        """
        F16-03: 造数 -> 设置 Storage Policy -> 快照
        目标：验证存储策略拦截逻辑。
        """
        test_file = f"{self.sandbox.test_dir}/policy_file.dat"
        create_test_file(self.runner, test_file)
        
        # 在 HDFS 侧，我们需要管理员权限设置策略
        res_h, res_o = self.runner.run_dual_hdfs_cmd("storagepolicies", "-setStoragePolicy", "-path", f"{{TARGET}}{test_file}", "-policy", "HOT")
        # storagepolicies 不是简单的 -action 形式，需要封装
        self.validator.assert_results_match(res_h, res_o, feature_tag="setStoragePolicy")
        
        self.sandbox.create_snapshot("snap_policy")