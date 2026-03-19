import pytest
import logging
import time
from dual_runner import DualHadoopCommandRunner, ParityValidator
from data_mutator import StressMutator

logger = logging.getLogger("TestGCResilience")

def test_delete_snapshot_interruption_resilience(runner, mutator):
    """
    场景: 在大规模数据快照删除过程中模拟中断。
    验证: 再次执行删除或后续检查时，不会产生永远无法被访问到的“孤兒对象”。
    由于服务端 GC 是异步的，通过注入超时模拟客户端在提交删除请求时的网络中断。
    """
    base_dir = "/reliability_gc_test"
    file_count = 50
    mutator.mass_create_files(base_dir, file_count)
    runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{base_dir}")
    runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{base_dir}", "snap_for_gc")
    
    # 模拟在删除快照时的网络超时 (标记删除请求可能已到达服务端，但客户端超时)
    logger.info("Injecting TIMEOUT during deleteSnapshot to simulate interruption...")
    runner.inject_fault(protocol="obs", error_type="TIMEOUT")
    hdfs_del, obs_del = runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{base_dir}", "snap_for_gc")
    
    assert hdfs_del.returncode == 0
    assert "INJECTED_TIMEOUT" in obs_del.stderr
    
    # 恢复后再次尝试删除，或者列出快照
    logger.info("Recovery: Re-trying deleteSnapshot to ensure consistency...")
    hdfs_retry, obs_retry = runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{base_dir}", "snap_for_gc")
    
    # OBS 侧即使第一次可能部分成功，第二次也应该能正确处理（幂等性或 NoSuchSnapshot）
    # HDFS 侧因为第一次已经成功了，第二次会报 NoSuchSnapshot
    assert hdfs_retry.returncode != 0
    
    # 最终结果：两者都不应该再看到该快照
    hdfs_ls, obs_ls = runner.run_dual_cmd("-ls", f"{{TARGET}}{base_dir}/.snapshot")
    assert "snap_for_gc" not in hdfs_ls.stdout
    assert "snap_for_gc" not in obs_ls.stdout
    
    # 清理
    runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{base_dir}")
