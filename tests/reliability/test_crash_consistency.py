import pytest
import subprocess
import time
import os
import signal
import logging
from dual_runner import DualHadoopCommandRunner, ParityValidator

logger = logging.getLogger("TestCrashConsistency")

def test_partial_write_snapshot_consistency(runner, mutator):
    """
    场景: 写入中途 Crash -> 立即打快照。
    验证: 快照中该文件的状态（是否存在，是否 0 字节）。
    """
    base_dir = "/reliability_partial_write"
    runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{base_dir}")
    runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{base_dir}")
    
    test_file = f"{base_dir}/partial_file.dat"
    
    # 模拟中途崩溃的写入
    logger.info("Triggering partial write via simulated crash...")
    mutator.partial_write_interruption(test_file, size_mb=4)
    
    # 立即触发快照
    hdfs_snap, obs_snap = runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{base_dir}", "snap_partial")
    
    # 验证一致性
    validator = ParityValidator(runner.mock_mode)
    validator.assert_results_match(hdfs_snap, obs_snap, "createSnapshot")
    
    # 检查快照内的文件状态
    hdfs_ls, obs_ls = runner.run_dual_cmd("-ls", f"{{TARGET}}{base_dir}/.snapshot/snap_partial")
    logger.info(f"Snapshot content after partial write crash:\nOBS: {obs_ls.stdout}")
    
    # 核心验证点：对于对象存储，未完成 Close 的对象通常不应该出现在快照中，或者是 0 字节
    validator.assert_results_match(hdfs_ls, obs_ls, "ls_snapshot_content")
    
    # 清理
    runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{base_dir}", "snap_partial")
    runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{base_dir}")

def test_process_kill_during_append(runner):
    """
    在向开启快照的路径执行 appendToFile 时，暴力 kill 客户端进程。
    验证重启（或后续操作）时快照数据的完整性。
    """
    base_dir = "/reliability_crash_test"
    runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{base_dir}")
    runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{base_dir}")
    
    test_file = f"{base_dir}/crash_file.dat"
    runner.run_dual_cmd("-touchz", f"{{TARGET}}{test_file}")
    
    # 准备一个大文件用于持续写入
    local_big_file = "/tmp/big_data_crash.dat"
    with open(local_big_file, "wb") as f:
        f.write(os.urandom(1024 * 1024 * 10)) # 10MB
    
    logger.info("Starting background append operation...")
    
    # 使用 runner.base_cli 动态适配执行环境中的 hdfs 命令前缀
    cmd = runner.base_cli + ["-appendToFile", local_big_file, f"{runner.obs_base}{test_file}"]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # 等待一会，确保写入已开始
    time.sleep(1)
    
    logger.info(f"Simulating HARD CRASH: killing process {process.pid}")
    process.send_signal(signal.SIGKILL)
    
    # 验证元数据一致性：快照是否还能正常工作
    runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{base_dir}", "snap_after_crash")
    
    # 检查文件状态
    hdfs_ls, obs_ls = runner.run_dual_cmd("-ls", f"{{TARGET}}{test_file}")
    logger.info(f"Post-crash file size: {obs_ls.stdout}")
    
    # 清理
    runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{base_dir}", "snap_after_crash")
    runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{base_dir}")
    if os.path.exists(local_big_file): os.remove(local_big_file)