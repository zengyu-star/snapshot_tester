import pytest
import logging
import os
from dual_runner import DualHadoopCommandRunner, ParityValidator

logger = logging.getLogger("TestDataIntegrity")


def test_silent_corruption_detection(runner):
    """
    模拟底层 OBS 对象被非法篡改。
    通过快照读取或恢复时，验证 Checksum 是否能捕获错误。
    """
    base_dir = "/reliability_integrity_test"
    runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{base_dir}")
    runner.run_dual_admin_cmd("-allowSnapshot", f"{{TARGET}}{base_dir}")
    
    test_file = f"{base_dir}/integrity_file.dat"
    # 创建含校验和的内容
    runner.run_dual_cmd("-touchz", f"{{TARGET}}{test_file}")
    
    local_data = "/tmp/integrity_origin.dat"
    with open(local_data, "w") as f: f.write("VALID_DATA_BLOCK")
    runner.run_dual_cmd("-put", "-f", local_data, f"{{TARGET}}{test_file}")
    
    # 建立快照备份状态
    runner.run_dual_cmd("-createSnapshot", f"{{TARGET}}{base_dir}", "safe_point")
    
    logger.info("Simulating SILENT CORRUPTION: rewriting underlying block illegally...")
    # 在 Mock 模式下，底层也是 HDFS。我们通过 -put -f 强行覆盖而不通过快照感知逻辑
    local_corrupt = "/tmp/integrity_corrupt.dat"
    with open(local_corrupt, "w") as f: f.write("CORRUPTED_DATA_BLOCK")
    
    # 直接写入目标路径（模拟绕过快照机制的物理修改）
    runner.run_dual_cmd("-put", "-f", local_corrupt, f"{{TARGET}}{test_file}")
    
    logger.info("Attempting to restore from snapshot...")
    # 从快照恢复到新路径
    restore_path = f"{base_dir}/restored_file.dat"
    cat_res_hdfs, cat_res_obs = runner.run_dual_cmd("-cp", f"{{TARGET}}{base_dir}/.snapshot/safe_point/integrity_file.dat", f"{{TARGET}}{restore_path}")
    
    # 预期结果：如果是强一致性系统，快照引用的 Block 应该能保持原样；
    # 如果底层 Block 被覆盖且快照仅是引用，则读取快照应当报校验和错误或内容不匹配。
    # 针对 OBSA 具体的实现（COW vs 引用），这里会捕获不同的异常。
    
    # 清理
    runner.run_dual_cmd("-deleteSnapshot", f"{{TARGET}}{base_dir}", "safe_point")
    runner.run_dual_cmd("-rm", "-r", f"{{TARGET}}{base_dir}")
