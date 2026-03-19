import pytest
import time
import logging
import os
from dual_runner import DualHadoopCommandRunner, ParityValidator
from data_mutator import StressMutator

logger = logging.getLogger("TestPerformanceOverhead")

def test_write_overhead_on_snapshotted_dir(runner, mutator):
    """
    验证开启快照后，对目录内文件的写入性能是否存在显著开销。
    在 POSIX 桶上，元数据快照理论上不应影响主路 IO。
    """
    base_dir = "/perf_overhead_test"
    runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{base_dir}")
    
    # 场景 1: 无快照基准
    logger.info("Baseline: Measuring write latency WITHOUT snapshot...")
    local_file = "/tmp/perf_base_10mb.dat"
    with open(local_file, "wb") as f:
        f.write(os.urandom(1024 * 1024 * 10)) # 10MB
        
    hdfs_res_base, obs_res_base = runner.run_dual_cmd("-put", "-f", local_file, f"{{TARGET}}{base_dir}/base.dat")
    base_latency = obs_res_base.duration_ms
    logger.info(f"Baseline OBS latency: {base_latency:.2f}ms")

    # 场景 2: 开启快照并存在多个历史版本
    runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{base_dir}")
    for i in range(5):
        runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{base_dir}", f"s{i}")
        # 产生一些碎片变异 (让元数据变厚)
        mutator.mass_create_files(f"{base_dir}/churn_{i}", 5)
        
    logger.info("Overhead Check: Measuring write latency WITH 5 active snapshots...")
    hdfs_res_snap, obs_res_snap = runner.run_dual_cmd("-put", "-f", local_file, f"{{TARGET}}{base_dir}/snap_test.dat")
    snap_latency = obs_res_snap.duration_ms
    logger.info(f"Snapshotted OBS latency: {snap_latency:.2f}ms")

    # 开销计算
    overhead_ratio = (snap_latency - base_latency) / base_latency if base_latency > 0 else 0
    logger.info(f"Calculated overhead: {overhead_ratio*100:.2f}%")
    
    # 阈值断言: 对 POSIX 桶，写开销应极低（基准设置 50%，因为 Mock 模式波动大，真实环境应 < 5%）
    assert overhead_ratio < 0.5, f"Write overhead too high: {overhead_ratio*100:.2f}%"

    # 清理
    runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{base_dir}")
    if os.path.exists(local_file): os.remove(local_file)
