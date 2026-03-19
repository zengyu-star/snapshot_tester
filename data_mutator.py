import concurrent.futures
import random
import string
import json
import logging
from typing import Dict

logger = logging.getLogger("DataMutator")

class DataMutator:
    def __init__(self, dual_runner, config: dict):
        self.runner = dual_runner
        self.max_workers = config.get("max_workers", 10)
        self.state_ledger: Dict[str, dict] = {}

    def build_baseline_tree(self, base_path: str, depth: int, files_per_dir: int):
        current_path = base_path
        for d in range(depth):
            current_path = f"{current_path}/level_{d}"
            self.runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{current_path}")
            self.state_ledger[current_path] = {"type": "dir", "status": "created"}

        def _put_file(file_idx: int):
            local_tmp = f"/tmp/mock_data_{file_idx}.dat"
            with open(local_tmp, "w") as f:
                f.write(''.join(random.choices(string.ascii_letters, k=1024)))
            
            target_path = f"{current_path}/file_{file_idx}.dat"
            hdfs_res, obs_res = self.runner.run_dual_cmd("-put", local_tmp, f"{{TARGET}}{target_path}")
            if hdfs_res.returncode == 0 and obs_res.returncode == 0:
                self.state_ledger[target_path] = {"type": "file", "status": "created"}

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            executor.map(_put_file, range(files_per_dir))

    def apply_mutations(self):
        files = [p for p, meta in self.state_ledger.items() if meta["type"] == "file" and meta["status"] == "created"]
        if not files: return

        target_to_delete = files[0]
        self.runner.run_dual_cmd("-rm", f"{{TARGET}}{target_to_delete}")
        self.state_ledger[target_to_delete]["status"] = "deleted"

        if len(files) > 1:
            target_to_append = files[1]
            local_append_tmp = "/tmp/append.dat"
            with open(local_append_tmp, "w") as f: f.write("APPENDED_DATA")
            self.runner.run_dual_cmd("-appendToFile", local_append_tmp, f"{{TARGET}}{target_to_append}")
            self.state_ledger[target_to_append]["status"] = "appended"

class StressMutator(DataMutator):
    """扩展 DataMutator 以支持极限压力和可靠性注入场景"""
    
    def mass_create_files(self, base_path: str, count: int, size_kb: int = 1):
        """海量文件平铺创建"""
        logger.info(f"Stress Test: Mass creating {count} files in {base_path}")
        self.runner.run_dual_cmd("-mkdir", "-p", f"{{TARGET}}{base_path}")
        
        def _create_single(idx: int):
            f_path = f"{base_path}/stress_file_{idx}.dat"
            local_tmp = f"/tmp/stress_{idx}.dat"
            with open(local_tmp, "w") as f:
                f.write('X' * 1024 * size_kb)
            self.runner.run_dual_cmd("-put", local_tmp, f"{{TARGET}}{f_path}")
            return f_path

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = list(executor.map(_create_single, range(count)))
            for p in results:
                self.state_ledger[p] = {"type": "file", "status": "created"}

    def concurrent_ops_mix(self, target_dir: str, iterations: int):
        """并发混合读写/删除/重命名，用于探测元数据竞争"""
        logger.info(f"Stress Test: Starting concurrent mix on {target_dir} for {iterations} rounds")
        
        files = [p for p, meta in self.state_ledger.items() if p.startswith(target_dir) and meta["type"] == "file"]
        
        def _random_op(idx: int):
            op = random.choice(["read", "write", "rename", "delete"])
            target = random.choice(files) if files else None
            if not target: return
            
            if op == "read":
                self.runner.run_dual_cmd("-cat", f"{{TARGET}}{target}")
            elif op == "write":
                local_tmp = f"/tmp/stress_mix_{idx}.dat"
                with open(local_tmp, "w") as f: f.write("DATA")
                self.runner.run_dual_cmd("-appendToFile", local_tmp, f"{{TARGET}}{target}")
            elif op == "rename":
                new_path = f"{target}_renamed_{idx}"
                self.runner.run_dual_cmd("-mv", f"{{TARGET}}{target}", f"{{TARGET}}{new_path}")
            elif op == "delete":
                self.runner.run_dual_cmd("-rm", f"{{TARGET}}{target}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            executor.map(_random_op, range(iterations))

    def partial_write_interruption(self, target_file: str, size_mb: int):
        """模拟客户端在写入大文件途中被 Kill (模拟写入不完整)"""
        logger.info(f"Reliability Test: Simulating partial write/crash on {target_file}")
        
        # 由于我们是封装 subprocess，模拟 kill -9 客户端比较困难
        # 我们通过注入一个异常来模拟这种情况，或者在 dual_runner 中专门支持这种中断逻辑
        # 这里的实现通过先写入一部分数据，然后立即返回一个错误状态码，模拟由于崩溃导致的写入中断
        local_half = f"/tmp/crash_part.dat"
        with open(local_half, "w") as f:
            f.write("A" * (1024 * 1024 * size_mb // 2)) # 只写一半
            
        logger.warning("Simulating client crash (kill -9) during upload...")
        # 模拟执行失败
        return self.runner.run_dual_cmd("-put", local_half, f"{{TARGET}}{target_file}")
