import pytest
import logging
import os
import sys

# 添加项目根目录到 sys.path，以便能找到 dual_runner 和 data_mutator 等模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_helpers import SnapshotSandbox, create_test_file
from dual_runner import ParityValidator

# 设置当前测试套件的日志记录器
logger = logging.getLogger("SnapshotLifecycleTest")

class TestSnapshotLifecycle:
    """
    快照核心生命周期端到端测试
    涵盖：赋权 -> 造数 -> 快照A -> 变异 -> 快照B -> 越权拦截
    """

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner, mutator, config):
        """
        使用 SnapshotSandbox 确保环境纯净
        """
        self.config = config
        self.runner = runner
        self.mutator = mutator
        self.test_dir = "/snap_lifecycle_box"
        
        self.sandbox = SnapshotSandbox(runner, "snap_lifecycle_box")
        self.sandbox.setup()
        
        yield
        
        self.sandbox.teardown()

    @pytest.mark.p0
    def test_full_lifecycle_parity(self):
        """主测试链路：验证 OBSA 插件的快照行为是否与原生 HDFS 达到像素级一致"""
        
        logger.info(">>> 步骤 1: 开启快照权限 (allowSnapshot)")
        # 注意：allowSnapshot 是管理员命令，必须使用 run_dual_admin_cmd
        hdfs_res, obs_res = self.runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{self.test_dir}")
        ParityValidator.assert_results_match(hdfs_res, obs_res)

        logger.info(">>> 步骤 2: 生成基线目录树和数据")
        # 造数模块会在刚才开辟的沙箱下创建 depth=2, 每个目录 3 个文件的拓扑结构
        self.mutator.build_baseline_tree(self.test_dir, depth=2, files_per_dir=3)

        logger.info(">>> 步骤 3: 创建基线快照 (snap_v1)")
        res_h, res_o = self.sandbox.create_snapshot("snap_v1")
        ParityValidator.assert_results_match(res_h, res_o)

        logger.info(">>> 步骤 4: 执行数据变异（随机删除、追加写入等）")
        self.mutator.apply_mutations()

        logger.info(">>> 步骤 5: 创建变异后快照 (snap_v2)")
        res_h, res_o = self.sandbox.create_snapshot("snap_v2")
        ParityValidator.assert_results_match(res_h, res_o)

        logger.info(">>> 步骤 6: 验证 .snapshot 隐藏目录的只读隔离性 (越权拦截测试)")
        # 故意试图向历史快照的只读目录里写数据
        snap_hidden_dir = f"{self.test_dir}/.snapshot/snap_v1"
        res_h, res_o = self.runner.run_dual_cmd("-touchz", f"{{TARGET}}{snap_hidden_dir}/illegal_file.txt")
        
        # 断言 1：HDFS 和 OBSA 都必须果断拦截（返回非 0 状态码）
        assert res_h.returncode != 0, "致命漏洞：原生 HDFS 防线被击穿！"
        assert res_o.returncode != 0, "致命漏洞：OBSA 插件允许向快照中写入脏数据！"
        
        # 断言 2：抛出的异常拦截信息必须一致
        ParityValidator.assert_results_match(res_h, res_o)

    @pytest.mark.p1
    def test_f1_02_rename_snapshot(self):
        """F1-02: renameSnapshot(A→X) -> 验证可见性"""
        self.sandbox.allow_snapshot()
        self.sandbox.create_snapshot("snap_A")
        
        res_h, res_o = self.runner.run_dual_cmd("-renameSnapshot", f"{{TARGET}}{self.test_dir}", "snap_A", "snap_X")
        # 跟踪重命名后的名，移除旧名
        if "snap_A" in self.sandbox._snapshots: self.sandbox._snapshots.remove("snap_A")
        self.sandbox._snapshots.append("snap_X")
        
        ParityValidator.assert_results_match(res_h, res_o)
        
        # 验证 X 可见，A 不可见
        ls_h, _ = self.runner.run_dual_cmd("-ls", f"{{TARGET}}{self.test_dir}/.snapshot")
        assert "snap_X" in ls_h.stdout
        assert "snap_A" not in ls_h.stdout

    @pytest.mark.p1
    def test_f1_03_delete_snapshot(self):
        """F1-03: deleteSnapshot(A) -> 验证消失"""
        self.sandbox.allow_snapshot()
        self.sandbox.create_snapshot("snap_to_del")
        
        res_h, res_o = self.sandbox.delete_snapshot("snap_to_del")
        ParityValidator.assert_results_match(res_h, res_o)
        
        ls_h, _ = self.runner.run_dual_cmd("-ls", f"{{TARGET}}{self.test_dir}/.snapshot")
        assert "snap_to_del" not in ls_h.stdout

    @pytest.mark.p1
    def test_f1_05_disallow_with_snapshots_fails(self):
        """F1-05: 有快照时 disallowSnapshot 必须被拦截"""
        self.sandbox.allow_snapshot()
        self.sandbox.create_snapshot("snap_exist")
        
        res_h, res_o = self.sandbox.disallow_snapshot()
        assert res_h.returncode != 0
        assert res_o.returncode != 0
        ParityValidator.assert_results_match(res_h, res_o)

    @pytest.mark.p1
    def test_f1_07_allow_idempotency(self):
        """F1-07: allowSnapshot 幂等性"""
        self.sandbox.allow_snapshot()
        res_h, res_o = self.sandbox.allow_snapshot()
        ParityValidator.assert_results_match(res_h, res_o)
        assert res_h.returncode == 0

    @pytest.mark.p1
    def test_f1_06_disallow_without_snapshots(self):
        """F1-06: 无快照时 disallowSnapshot 正常执行"""
        self.sandbox.allow_snapshot()
        res_h, res_o = self.sandbox.disallow_snapshot()
        ParityValidator.assert_results_match(res_h, res_o)
        assert res_h.returncode == 0

    @pytest.mark.p1
    def test_f1_08_create_snapshot_unauthorized(self):
        """F1-08: 未授权时创建快照错误语义一致"""
        # 确保未开启快照权限
        self.sandbox.disallow_snapshot()
        
        # 直接调用 runner 因为创建快照会抛异常，sandbox 内部 tracking 可能会乱
        res_h, res_o = self.runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{self.test_dir}", "snap_unauth")
        assert res_h.returncode != 0
        assert res_o.returncode != 0
        ParityValidator.assert_results_match(res_h, res_o)

    @pytest.mark.p1
    def test_f1_09_delete_snapshot_isolation(self):
        """F1-09: 删除一个快照不影响另一个"""
        self.sandbox.allow_snapshot()
        self.sandbox.create_snapshot("snap_1")
        self.sandbox.create_snapshot("snap_2")
        
        # 删除 snap_1
        self.sandbox.delete_snapshot("snap_1")
        
        # 验证 snap_2 依然存在
        ls_h, _ = self.runner.run_dual_cmd("-ls", f"{{TARGET}}{self.test_dir}/.snapshot")
        assert "snap_2" in ls_h.stdout
        assert "snap_1" not in ls_h.stdout