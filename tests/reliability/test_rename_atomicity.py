import pytest
import logging
from data_mutator import StressMutator

logger = logging.getLogger("TestRenameAtomicity")

def test_concurrent_rename_and_snapshot(runner, mutator):
    """
    场景: 在大规模目录重命名 (Rename) 的瞬间触发快照。
    验证: POSIX 桶的原子性保证，快照内元数据的一致性（要么是旧路径，要么是新路径，不存在中间态）。
    """
    parent_dir = "/reliability_rename_atomicity"
    old_dir = f"{parent_dir}/old_subtree"
    new_dir = f"{parent_dir}/new_subtree"
    
    # 构造一批数据
    mutator.mass_create_files(old_dir, count=100)
    runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{parent_dir}")
    
    logger.info(f"Triggering concurrent rename {old_dir} -> {new_dir} and createSnapshot...")
    
    # 我们通过并发交织来模拟
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(runner.run_dual_cmd, "-mv", f"{{TARGET}}{old_dir}", f"{{TARGET}}{new_dir}")
        f2 = executor.submit(runner.run_dual_cmd, "-createSnapshot", f"{{TARGET}}{parent_dir}", "snap_rename")
        
        res_mv_hdfs, res_mv_obs = f1.result()
        res_snap_hdfs, res_snap_obs = f2.result()

    logger.info(f"Rename Result (OBS): {res_mv_obs.returncode}, Snapshot Result (OBS): {res_snap_obs.returncode}")
    
    # 验证一致性
    validator = ParityValidator(runner.mock_mode)
    validator.assert_results_match(res_snap_hdfs, res_snap_obs, "createSnapshot")
    
    # 检查快照内容，对应重命名后的状态
    hdfs_ls, obs_ls = runner.run_dual_cmd("-ls", "-R", f"{{TARGET}}{parent_dir}/.snapshot/snap_rename")
    validator.assert_results_match(hdfs_ls, obs_ls, "ls_snapshot_content")
    
    # 清理
    runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{parent_dir}")
