import pytest
import logging
import yaml
import os
import sys

# 添加项目根目录到 sys.path，以便能找到 dual_runner 和 data_mutator 等模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dual_runner import DualHadoopCommandRunner, ParityValidator
from data_mutator import DataMutator

# 设置当前测试套件的日志记录器
logger = logging.getLogger("SnapshotLifecycleTest")

class TestSnapshotLifecycle:
    """
    快照核心生命周期端到端测试
    涵盖：赋权 -> 造数 -> 快照A -> 变异 -> 快照B -> 差异比对 -> 越权拦截
    """

    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        """
        严苛的防污染沙箱环境准备与清理
        """
        # ================= 加载配置 =================
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(project_root, "config.yml")
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)
        
        # ================= 配置与初始化 =================
        # 从 config.yml 中读取基础路径
        nn = self.config['cluster_env']['hadoop_namenode'].rstrip('/')
        base = self.config['cluster_env']['test_base_path'].strip('/')
        
        hdfs_test_base = f"{nn}/{base}/hdfs_side"
        # 即使在 mock 模式下，这里也先写 obs://，由 runner 内部处理映射
        obs_test_base = f"obs://{self.config['cluster_env']['obs_bucket']}/{base}/obs_side"
        
        # 初始化双端执行引擎和造数模块
        self.runner = DualHadoopCommandRunner(hdfs_test_base, obs_test_base, self.config)
        self.mutator = DataMutator(self.runner, self.config.get("data_model", {}))
        
        # 定义测试目录的相对路径
        self.test_dir = "/snap_lifecycle_01"

        # ================= Setup 阶段 =================
        logger.info(f"=== Setup: 初始化双端测试沙箱 ===")
        # 强行清理可能存在的历史残留残留目录，并重建干净的测试基座
        self.runner.run_dual_cmd("-rm", "-r", "-f", f"{{TARGET}}{self.test_dir}")
        self.runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{self.test_dir}")

        yield  # 移交控制权，开始执行下方的 test_ 核心用例

        # ================= Teardown 阶段 =================
        logger.info(f"=== Teardown: 执行物理级清理 ===")
        # 清理顺序严格规定：删快照 -> 解除快照权限 -> 删物理目录
        # 即使用例执行中途崩溃，这里也会忠实执行，保证下次运行环境纯洁
        self.runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{self.test_dir}", "snap_v1")
        self.runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{self.test_dir}", "snap_v2")
        self.runner.run_dual_admin_cmd("-disallowSnapshot", f"{{TARGET}}{self.test_dir}")
        self.runner.run_dual_cmd("-rm", "-r", "-f", f"{{TARGET}}{self.test_dir}")
        logger.info("=== 测试沙箱清理完毕 ===")

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
        res_h, res_o = self.runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{self.test_dir}", "snap_v1")
        ParityValidator.assert_results_match(res_h, res_o)

        logger.info(">>> 步骤 4: 执行数据变异（随机删除、追加写入等）")
        self.mutator.apply_mutations()

        logger.info(">>> 步骤 5: 创建变异后快照 (snap_v2)")
        res_h, res_o = self.runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{self.test_dir}", "snap_v2")
        ParityValidator.assert_results_match(res_h, res_o)

        logger.info(">>> 步骤 6: 验证 snapshotDiff 差异树的结构一致性")
        # 强制比对 Hadoop 的 M, +, - 等差异标记符是否分毫不差
        res_h, res_o = self.runner.run_dual_hdfs_cmd("snapshotDiff", f"{{TARGET}}{self.test_dir}", "snap_v1", "snap_v2")
        ParityValidator.assert_results_match(res_h, res_o, strict_error_match=True)

        logger.info(">>> 步骤 7: 验证 .snapshot 隐藏目录的只读隔离性 (越权拦截测试)")
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
        self.runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{self.test_dir}")
        self.runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{self.test_dir}", "snap_A")
        
        res_h, res_o = self.runner.run_dual_cmd("-renameSnapshot", f"{{TARGET}}{self.test_dir}", "snap_A", "snap_X")
        ParityValidator.assert_results_match(res_h, res_o)
        
        # 验证 X 可见，A 不可见
        ls_h, _ = self.runner.run_dual_cmd("-ls", f"{{TARGET}}{self.test_dir}/.snapshot")
        assert "snap_X" in ls_h.stdout
        assert "snap_A" not in ls_h.stdout

    @pytest.mark.p1
    def test_f1_03_delete_snapshot(self):
        """F1-03: deleteSnapshot(A) -> 验证消失"""
        self.runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{self.test_dir}")
        self.runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{self.test_dir}", "snap_to_del")
        
        res_h, res_o = self.runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{self.test_dir}", "snap_to_del")
        ParityValidator.assert_results_match(res_h, res_o)
        
        ls_h, _ = self.runner.run_dual_cmd("-ls", f"{{TARGET}}{self.test_dir}/.snapshot")
        assert "snap_to_del" not in ls_h.stdout

    @pytest.mark.p1
    def test_f1_05_disallow_with_snapshots_fails(self):
        """F1-05: 有快照时 disallowSnapshot 必须被拦截"""
        self.runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{self.test_dir}")
        self.runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{self.test_dir}", "snap_exist")
        
        res_h, res_o = self.runner.run_dual_admin_cmd("-disallowSnapshot", f"{{TARGET}}{self.test_dir}")
        assert res_h.returncode != 0
        assert res_o.returncode != 0
        ParityValidator.assert_results_match(res_h, res_o)

    @pytest.mark.p1
    def test_f1_07_allow_idempotency(self):
        """F1-07: allowSnapshot 幂等性"""
        self.runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{self.test_dir}")
        res_h, res_o = self.runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{self.test_dir}")
        ParityValidator.assert_results_match(res_h, res_o)
        assert res_h.returncode == 0
