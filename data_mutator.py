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
