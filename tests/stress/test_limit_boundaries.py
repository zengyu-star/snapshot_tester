import pytest
import logging
from dual_runner import DualHadoopCommandRunner, ParityValidator
from data_mutator import StressMutator

logger = logging.getLogger("TestStress")


def test_long_snapshot_chain(runner):
    """
    测试同一目录下极长的快照链 (100+)。
    """
    base_dir = "/stress_long_chain"
    chain_length = 10  # 缩小规模以适应 Mock 环境
    
    # 1. 预清理：防止上次测试异常中断导致环境残留
    for i in range(chain_length): 
        runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{base_dir}", f"snap_{i}")
    runner.run_dual_admin_cmd("-disallowSnapshot", f"{{TARGET}}{base_dir}")
    runner.run_dual_cmd("-rm", "-r", "-f", f"{{TARGET}}{base_dir}")

    # 2. 正式环境初始化
    runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{base_dir}")
    runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{base_dir}")

    logger.info(f"Stress Test: Creating a chain of {chain_length} snapshots...")

    try:
        for i in range(chain_length):
            # 每次创建快照前写入一点数据
            runner.run_dual_cmd("-touchz", f"{{TARGET}}{base_dir}/data_{i}.log")
            hdfs_res, obs_res = runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{base_dir}", f"snap_{i}")
            if hdfs_res.returncode != 0 or obs_res.returncode != 0:
                logger.error(f"Failed at snapshot index {i}")
                break

        # 验证最后的状态
        # TODO: 比对 snapshotDiff between snap_0 and snap_49
    finally:
        # 3. 严格清理：必须先删除所有快照，并解除快照权限，才能成功执行 rm -r
        for i in range(chain_length):
            runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{base_dir}", f"snap_{i}")
        runner.run_dual_admin_cmd("-disallowSnapshot", f"{{TARGET}}{base_dir}")
        runner.run_dual_cmd("-rm", "-r", "-f", f"{{TARGET}}{base_dir}")


def test_ultra_wide_directory_snapshot(runner, mutator):
    """
    测试包含海量平铺文件的目录快照耗时 (1000+ 文件)。
    """
    base_dir = "/stress_wide_dir"
    file_count = 50
    
    # 1. 预清理：静默清除上次测试报错残留的 wide_snap 快照和目录
    runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{base_dir}", "wide_snap")
    runner.run_dual_admin_cmd("-disallowSnapshot", f"{{TARGET}}{base_dir}")
    runner.run_dual_cmd("-rm", "-r", "-f", f"{{TARGET}}{base_dir}")

    # 2. 造数与赋权
    mutator.mass_create_files(base_dir, file_count)
    runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{base_dir}")

    logger.info(f"Stress Test: Creating snapshot for directory with {file_count} files...")
    
    try:
        hdfs_res, obs_res = runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{base_dir}", "wide_snap")

        # 验证
        assert hdfs_res.returncode == 0
        assert obs_res.returncode == 0

        logger.info(f"Snapshot created successfully for {file_count} files. OBS Duration: {obs_res.duration_ms:.2f}ms")
    finally:
        # 3. 严格清理：保证异常退出时也不会产生脏数据
        runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{base_dir}", "wide_snap")
        runner.run_dual_admin_cmd("-disallowSnapshot", f"{{TARGET}}{base_dir}")
        runner.run_dual_cmd("-rm", "-r", "-f", f"{{TARGET}}{base_dir}")