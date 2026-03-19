import pytest
import logging
from dual_runner import DualHadoopCommandRunner, ParityValidator
from data_mutator import DataMutator

logger = logging.getLogger("TestNetworkFaults")

def test_snapshot_create_with_timeout_sim(runner, validator):
    """
    模拟网络延迟或超时情况下创建快照。
    使用 inject_fault 机制，无论是在 Mock 还是真实 OBS 环境下都能一致验证容错逻辑。
    """
    base_dir = "/reliability_network_test"
    runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{base_dir}")
    runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{base_dir}")
    
    logger.info("Injecting a simulated TIMEOUT on the OBS path...")
    runner.inject_fault(protocol="obs", error_type="TIMEOUT")
    
    # 执行命令，期待 OBS 侧失败（因为注入了故障）
    hdfs_res, obs_res = runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{base_dir}", "s1")
    
    # 验证逻辑：HDFS 应该成功 (code=0)，而 OBS 应该返回我们注入的超时错误
    assert hdfs_res.returncode == 0
    assert "INJECTED_TIMEOUT" in obs_res.stderr
    
    logger.info("Fault injection verified. Cleaning up...")
    # 手动清理（因为 OBS 侧其实没创建成功，但 HDFS 侧创建了）
    runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{base_dir}", "s1")
    runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{base_dir}")

def test_recovery_after_simulated_partition(runner, validator):
    """
    模拟网络恢复后的状态一致性。
    在注入故障导致创建失败后，验证后续正常请求是否能使状态恢复一致。
    """
    base_dir = "/reliability_recovery_test"
    runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{base_dir}")
    runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{base_dir}")
    
    logger.info("Outage: Injecting TIMEOUT for OBSA specifically.")
    runner.inject_fault(protocol="obs", error_type="TIMEOUT")
    
    # OBSA 侧会失败，HDFS 侧会成功。这会导致“元数据裂脑”
    runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{base_dir}", "ghost_snap")
    
    logger.info("Recovery: Normal operation should still work or report clear status.")
    # 再次创建同名快照。HDFS 应该报 AlreadyExists，OBSA 应该正常创建（因为它之前没成功）
    # 或者，我们尝试删除这个“幽灵快照”。
    hdfs_del, obs_del = runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{base_dir}", "ghost_snap")
    
    # 此时 HDFS 应该删除了，OBSA 报 NoSuchSnapshot 是正常的，因为之前就没建出来
    # 重要的是：最终两者在“快照不存在”这一点上达成一致
    
    # 验证最终一致性
    hdfs_ls, obs_ls = runner.run_dual_cmd("-ls", f"{{TARGET}}{base_dir}/.snapshot")
    assert "ghost_snap" not in hdfs_ls.stdout
    assert "ghost_snap" not in obs_ls.stdout
    
    logger.info("State converged back to consistency. Cleaning up...")
    runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{base_dir}")
def test_throttling_resilience(runner, validator):
    """
    场景: OBS 服务端返回 429 (Slow Down) 或 503 (Service Unavailable)。
    验证: OBSA 插件的重试表现及最终错误透传。
    """
    base_dir = "/reliability_throttling_test"
    runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{base_dir}")
    runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{base_dir}")
    
    # 场景 1: 模拟 429 流控
    logger.info("Injecting SIMULATED 429 THROTTLING...")
    runner.inject_fault(protocol="obs", error_type="THROTTLING_429")
    hdfs_res, obs_res = runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{base_dir}", "snap_429")
    
    assert hdfs_res.returncode == 0
    assert "429 Too Many Requests" in obs_res.stderr
    
    # 场景 2: 模拟 503 服务不可用
    logger.info("Injecting SIMULATED 503 SERVER ERROR...")
    runner.inject_fault(protocol="obs", error_type="SERVER_ERROR_503")
    hdfs_res, obs_res = runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{base_dir}", "snap_503")
    
    assert hdfs_res.returncode == 0
    assert "503 Service Unavailable" in obs_res.stderr

    # 清理
    runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{base_dir}", "snap_429")
    runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{base_dir}", "snap_503")
    runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{base_dir}")
