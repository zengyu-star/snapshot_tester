import pytest
import logging
import time
import subprocess
from test_helpers import SnapshotSandbox, create_test_file

logger = logging.getLogger("TestUnsupported")

class TestUnsupportedConcurrency:
    """
    F15: 快照 × 并发与状态冻结隔离 (Lease / Concat)
    针对 OBSA 不支持的 Lease 和 Concat 语义进行边界测试。
    """

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner, validator):
        self.runner = runner
        self.validator = validator
        self.sandbox = SnapshotSandbox(runner, "f15_unsupported_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    @pytest.mark.p2
    def test_f15_01_snapshot_during_open_stream(self):
        """
        F15-01: 打开文件流写入一半数据(不close) -> 双端打快照 -> close流 -> 读取快照
        目标：验证不支持 Lease 时的快照边界。
        HDFS 预期：快照捕获到 flush 后但在 close 前的数据。
        OBSA 预期：由于不支持 Lease，可能导致快照中文件不可见或 0 字节，需验证不产生静默损坏。
        """
        test_file = f"{self.sandbox.test_dir}/open_file.dat"
        
        # 模拟一个持续写入的流 (使用 hdfs dfs -put -)
        # 我们启动两个背景进程，分别向 HDFS 和 OBS 写入
        def start_hanging_put(base_uri):
            # 使用 local hdfs 脚本
            cmd = ["./hdfs", "dfs", "-put", "-", f"{base_uri}{test_file}"]
            proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            proc.stdin.write("HEADING_DATA_")
            proc.stdin.flush()
            return proc

        logger.info(">>> 启动后台写入进程 (保持流开启)")
        hdfs_proc = start_hanging_put(self.runner.hdfs_base)
        obs_proc = start_hanging_put(self.runner.obs_base)
        
        try:
            time.sleep(2) # 等待流建立
            
            logger.info(">>> 在流未关闭时创建快照")
            res_h, res_o = self.sandbox.create_snapshot("snap_during_write")
            # 这里我们不直接用 validator.assert_results_match，因为 Lease 差异是预期的
            # 但 HDFS 应该是成功的
            assert res_h.returncode == 0
            
            # OBSA 侧：可能成功（静默过），也可能失败。
            # 如果成功，我们要看后面读出来的东西。
        finally:
            logger.info(">>> 关闭流")
            hdfs_proc.stdin.close()
            obs_proc.stdin.close()
            hdfs_proc.wait()
            obs_proc.wait()

        # 读取快照中的内容
        cat_h, cat_o = self.runner.run_dual_cmd("-cat", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_during_write/open_file.dat")
        
        # 一致性校验
        # 在 Mock 模式下，两者应一致（都读到 HEADING_DATA_）
        # 在实测模式下，我们允许不一致，但要求双端都不能崩溃
        self.validator.assert_results_match(cat_h, cat_o, feature_tag="lease_interaction")

    @pytest.mark.p2
    def test_f15_02_concat_snapshot_isolation(self):
        """
        F15-02: 造数 -> concat 合并源文件到目标文件 -> 打快照
        目标：验证 concat 命令在 OBSA 下的拦截行为。
        """
        create_test_file(self.runner, f"{self.sandbox.test_dir}/target.dat", "TARGET_BASE_")
        create_test_file(self.runner, f"{self.sandbox.test_dir}/source.dat", "SOURCE_DATA")
        
        # 执行 concat
        res_h, res_o = self.runner.run_dual_cmd("-concat", f"{{TARGET}}{self.sandbox.test_dir}/target.dat", f"{{TARGET}}{self.sandbox.test_dir}/source.dat")
        
        # 验证拦截逻辑
        self.validator.assert_results_match(res_h, res_o, feature_tag="concat")
        
        # 如果 concat 成功（HDFS 侧），验证快照
        self.sandbox.create_snapshot("after_concat")
        res_cat_h, res_cat_o = self.runner.run_dual_cmd("-cat", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/after_concat/target.dat")
        self.validator.assert_results_match(res_cat_h, res_cat_o, feature_tag="concat_content")
