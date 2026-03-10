import subprocess
import logging
import sys
import os

# 把项目根路径加进sys.path，防止在没包装好的环境下找不到 utils 模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dataclasses import dataclass
from typing import Tuple, List
from utils.log_config import setup_logging

# 使用我们新的日志框架
setup_logging()
logger = logging.getLogger("DualRunner")

@dataclass
class CmdResult:
    protocol: str
    command: str
    returncode: int
    stdout: str
    stderr: str

class DualHadoopCommandRunner:
    def __init__(self, hdfs_base_uri: str, obs_base_uri: str, config: dict = None):
        self.config = config or {}
        self.mock_mode = self.config.get("global", {}).get("mock_obsa_mode", False)
        
        self.hdfs_base = hdfs_base_uri.rstrip('/')
        if self.mock_mode and obs_base_uri.startswith("obs://"):
            # 在 Mock 模式下，将 obs://bucket/path 转换为 hdfs://namenode:8020/mock_bucket/path
            # 这样即使没有安装 OBSA 插件，也能使用标准 HDFS 模拟比对逻辑
            import re
            match = re.match(r"(hdfs://[^/]+)", self.hdfs_base)
            nn_prefix = match.group(1) if match else ""
            self.obs_base = obs_base_uri.replace("obs://", f"{nn_prefix}/mock_").rstrip('/')
            logger.info(f"Mock OBSA Mode: Redirecting {obs_base_uri} -> {self.obs_base}")
        else:
            self.obs_base = obs_base_uri.rstrip('/')

        # 优先使用本地工作的 hdfs 脚本 (./hdfs)，如果没有则退化到系统 path 中的 hdfs
        import os
        local_hdfs = os.path.join(os.getcwd(), "hdfs")
        self.base_cli = [local_hdfs, "dfs"] if os.path.exists(local_hdfs) else ["hdfs", "dfs"]
        self.admin_cli = [local_hdfs, "dfsadmin"] if os.path.exists(local_hdfs) else ["hdfs", "dfsadmin"]
        # 顶层 hdfs 命令（如 snapshotDiff），不经过 dfs/dfsadmin 子命令
        self.hdfs_cli = [local_hdfs] if os.path.exists(local_hdfs) else ["hdfs"]

    def _execute(self, cmd_list: List[str], protocol: str) -> CmdResult:
        full_cmd_str = " ".join(cmd_list)
        logger.debug(f"Executing [{protocol.upper()}]: {full_cmd_str}")
        
        try:
            result = subprocess.run(
                cmd_list, capture_output=True, text=True, timeout=300
            )
            return CmdResult(
                protocol=protocol, command=full_cmd_str,
                returncode=result.returncode,
                stdout=result.stdout.strip(), stderr=result.stderr.strip()
            )
        except subprocess.TimeoutExpired:
            logger.error(f"Command timed out after 300s: {full_cmd_str}")
            return CmdResult(protocol, full_cmd_str, -1, "", "TIMEOUT")

    def run_dual_cmd(self, action: str, *args) -> Tuple[CmdResult, CmdResult]:
        """使用 {TARGET} 占位符动态组装标准命令"""
        hdfs_cmd = self.base_cli + [action]
        obs_cmd = self.base_cli + [action]
        for arg in args:
            if "{TARGET}" in arg:
                hdfs_cmd.append(arg.replace("{TARGET}", self.hdfs_base))
                obs_cmd.append(arg.replace("{TARGET}", self.obs_base))
            else:
                hdfs_cmd.append(arg)
                obs_cmd.append(arg)
        return self._execute(hdfs_cmd, "hdfs"), self._execute(obs_cmd, "obs")

    def run_dual_admin_cmd(self, action: str, *args) -> Tuple[CmdResult, CmdResult]:
        """使用 {TARGET} 占位符动态组装 dfsadmin 命令"""
        hdfs_cmd = self.admin_cli + [action]
        obs_cmd = self.admin_cli + [action]
        for arg in args:
            if "{TARGET}" in arg:
                hdfs_cmd.append(arg.replace("{TARGET}", self.hdfs_base))
                obs_cmd.append(arg.replace("{TARGET}", self.obs_base))
            else:
                hdfs_cmd.append(arg)
                obs_cmd.append(arg)
        return self._execute(hdfs_cmd, "hdfs"), self._execute(obs_cmd, "obs")

    def run_dual_hdfs_cmd(self, subcmd: str, *args) -> Tuple[CmdResult, CmdResult]:
        """使用 {TARGET} 占位符动态组装顶层 hdfs 命令（如 snapshotDiff）"""
        hdfs_cmd = self.hdfs_cli + [subcmd]
        obs_cmd = self.hdfs_cli + [subcmd]
        for arg in args:
            if "{TARGET}" in arg:
                hdfs_cmd.append(arg.replace("{TARGET}", self.hdfs_base))
                obs_cmd.append(arg.replace("{TARGET}", self.obs_base))
            else:
                hdfs_cmd.append(arg)
                obs_cmd.append(arg)
        return self._execute(hdfs_cmd, "hdfs"), self._execute(obs_cmd, "obs")

class ParityValidator:
    # OBSA 官方明确不支持的功能特性
    UNSUPPORTED_OBSA_FEATURES = [
        "setfacl", "getfacl", "setfattr", "getfattr", 
        "concat", "ln", "setSpaceQuota", "clrSpaceQuota", 
        "setQuota", "clrQuota", "setStoragePolicy",
        "moveToLocal" # <--- 已将 "fsck" 从列表中移除
    ]

    def __init__(self, is_mock_mode: bool = False):
        self.is_mock_mode = is_mock_mode

    def assert_results_match(self, hdfs_res: CmdResult, obs_res: CmdResult, feature_tag: str = None, strict_error_match: bool = False):
        """
        比对 HDFS 和 OBSA 的执行结果。
        如果 feature_tag 在不支持列表中且处于非 Mock 模式，则验证 OBSA 是否正确拦截（返回非 0）。
        """
        # 1. 处理已知不支持的特性逻辑 (仅在非 Mock 模式下生效)
        if not self.is_mock_mode and feature_tag in self.UNSUPPORTED_OBSA_FEATURES:
            logger.info(f"检测到已知不支持特性: {feature_tag}，切换为拦截验证模式。")
            if obs_res.returncode == 0:
                error_msg = f"严重：OBSA 静默通过了不支持的特性 {feature_tag}！这可能导致元数据不一致。"
                logger.error(error_msg)
                raise AssertionError(error_msg)
            
            # 对于不支持的特性，我们不强求 stderr 完全一致，只要 OBSA 报错即可
            # 但 HDFS 侧通常是成功的（因为 HDFS 支持），所以 returncode 肯定不一致，这里提前返回
            logger.info(f"OBSA 已正确拦截不支持的特性 {feature_tag} (code={obs_res.returncode})。")
            return

        # 2. 标准一致性检查
        if hdfs_res.returncode != obs_res.returncode:
            error_msg = (
                f"状态码不一致! [Feature: {feature_tag}]\n"
                f"HDFS 返回: {hdfs_res.returncode} | OBSA 返回: {obs_res.returncode}\n"
                f"HDFS Stdout: {hdfs_res.stdout} | OBSA Stdout: {obs_res.stdout}\n"
                f"HDFS Stderr: {hdfs_res.stderr} | OBSA Stderr: {obs_res.stderr}"
            )
            logger.error(error_msg)
            raise AssertionError(error_msg)

        # 路径替换逻辑，用于消除 Stdout 中的绝对路径差异
        cleaned_hdfs_stdout = hdfs_res.stdout.replace(hdfs_res.command.split()[-1], "TARGET_PATH")
        cleaned_obs_stdout = obs_res.stdout.replace(obs_res.command.split()[-1], "TARGET_PATH")
        
        if cleaned_hdfs_stdout != cleaned_obs_stdout:
            logger.warning(f"发现 Stdout 差异 [Feature: {feature_tag}]，可能是路径前缀导致，请人工 Review。")

        if strict_error_match and hdfs_res.returncode != 0:
            # 在某些高危场景，不仅要求返回码一致，还要求异常类型一致
            if "SnapshotException" in hdfs_res.stderr and "SnapshotException" not in obs_res.stderr:
                raise AssertionError(f"OBSA 插件未抛出预期的 SnapshotException！Stderr: {obs_res.stderr}")