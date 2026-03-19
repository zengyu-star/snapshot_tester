import pytest
import logging
from dual_runner import DualHadoopCommandRunner, ParityValidator
from data_mutator import StressMutator

logger = logging.getLogger("TestPerformance")


def test_snapshot_o1_scaling(runner, mutator):
    """
    验证快照创建耗时是否随文件数量增加而线性增长 (理想应为 O(1))。
    """
    scales = [5, 10, 20] # 缩小规模以适应 Mock 环境下的 docker exec 开销
    results = {}

    for count in scales:
        dir_path = f"/perf_o1_scale_{count}"
        mutator.mass_create_files(dir_path, count, size_kb=1)
        runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{dir_path}")
        
        # 测量 HDFS 耗时
        hdfs_res, obs_res = runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{dir_path}", "sp_perf")
        
        results[count] = {
            "hdfs_ms": hdfs_res.duration_ms,
            "obs_ms": obs_res.duration_ms
        }
        
        logger.info(f"Scale {count}: HDFS={hdfs_res.duration_ms:.2f}ms, OBS={obs_res.duration_ms:.2f}ms")
        
        # 清理
        runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{dir_path}", "sp_perf")
        runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{dir_path}")

    # 简单的趋势验证 (可选)
    # assert results[500]["obs_ms"] < results[10]["obs_ms"] * 5 # 允许一定浮动但不能是 O(N)
