import pytest
import logging
import os
import sys
import subprocess
import time

# 添加项目根目录到 sys.path，以便正常导入框架模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_helpers import SnapshotSandbox, create_test_file
from dual_runner import ParityValidator

logger = logging.getLogger("TestUnsupported")

@pytest.mark.p2
class TestUnsupportedConcurrency:
    """F15: 不支持特性与并发边界测试"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner):
        self.runner = runner
        self.sandbox = SnapshotSandbox(runner, "f15_unsupported_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    def test_f15_01_snapshot_during_open_stream(self):
        """
        F15-01: 打开文件流写入一半数据(不close) -> 双端打快照 -> close流 -> 读取快照
        目标：验证不支持 Lease 时的快照边界。
        HDFS 预期：快照捕获到 flush 后但在 close 前的数据。
        OBSA 预期：由于不支持 Lease，可能导致快照中文件不可见或 0 字节，需验证不产生静默损坏。
        """
        test_file = f"{self.sandbox.test_dir}/open_file.dat"

        # 模拟一个持续写入的流 (使用 hdfs dfs -put -)
        def start_hanging_put(base_uri):
            # 动态使用 runner 的基础命令前缀，兼容 Mock 与真实环境
            cmd = self.runner.base_cli + ["-put", "-", f"{base_uri}{test_file}"]
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            proc.stdin.write("HEADING_DATA_")
            proc.stdin.flush()
            return proc

        logger.info(">>> 启动后台写入进程 (保持流开启)")
        hdfs_proc = start_hanging_put(self.runner.hdfs_base)
        obs_proc = start_hanging_put(self.runner.obs_base)
        
        # 稍作等待确保流已建立并触发 Flush
        time.sleep(2)
        
        logger.info(">>> 在流开启状态下创建快照")
        self.sandbox.create_snapshot("snap_mid_stream")
        
        logger.info(">>> 关闭流，完成写入")
        # 写入剩余数据并安全关闭流
        hdfs_proc.stdin.write("TAIL_DATA\n")
        hdfs_proc.stdin.close()
        obs_proc.stdin.write("TAIL_DATA\n")
        obs_proc.stdin.close()
        
        # 等待后台进程安全结束
        hdfs_proc.wait(timeout=15)
        obs_proc.wait(timeout=15)
        
        logger.info(">>> 验证活跃文件内容一致性")
        # 验证最终活跃文件是否一致
        res_h, res_o = self.runner.run_dual_cmd("-cat", f"{{TARGET}}{test_file}")
        
        # [核心修复点] 实例化后再调用，兼容底层 assert_results_match 的实例方法/静态方法定义
        ParityValidator().assert_results_match(res_h, res_o)
        
        logger.info(">>> 验证快照防损坏边界")
        # 对于不支持 Lease 续约的 OBSA，快照中该文件可能不完整甚至不存在
        # 我们核心断言系统没有崩溃且不产生底层错误
        res_h_snap, res_o_snap = self.runner.run_dual_cmd("-ls", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_mid_stream/")
        assert res_h_snap.returncode == 0, "HDFS 快照读取失败"
        assert res_o_snap.returncode == 0, "OBSA 快照读取失败，可能发生了静默损坏"

    def test_f15_02_concat_snapshot_isolation(self):
        """
        F15-02: concat 不支持特性验证
        HDFS 视版本支持 concat，OBSA 明确不支持，测试底层框架的拦截兼容性
        """
        create_test_file(self.runner, f"{self.sandbox.test_dir}/c1.dat", "DATA1")
        create_test_file(self.runner, f"{self.sandbox.test_dir}/c2.dat", "DATA2")
        self.sandbox.create_snapshot("s1")
        
        # 发起 concat 操作
        res_h, res_o = self.runner.run_dual_cmd("-concat", f"{{TARGET}}{self.sandbox.test_dir}/c1.dat", f"{{TARGET}}{self.sandbox.test_dir}/c2.dat")
        
        # 验证 OBSA 正确拦截（根据日志应返回 code 255）
        assert res_o.returncode != 0, f"OBSA 必须拦截不支持的 concat 操作，实际 stderr: {res_o.stderr}"
        
        # 验证拦截行为没有破坏历史快照
        res_h_snap, res_o_snap = self.runner.run_dual_cmd("-cat", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/s1/c1.dat")
        
        # [核心修复点] 实例化后再调用
        ParityValidator().assert_results_match(res_h_snap, res_o_snap)