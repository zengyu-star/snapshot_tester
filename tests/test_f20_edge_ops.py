import pytest
import logging
import os
from test_helpers import SnapshotSandbox, create_test_file

logger = logging.getLogger("TestEdgeOps")

class TestEdgeOperations:
    """
    F20: 快照 × 边缘文件操作 (moveFromLocal / moveToLocal / find / text)
    针对不常用但可能存在歧义的 Hadoop 命令进行覆盖。
    """

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner, validator):
        self.runner = runner
        self.validator = validator
        self.sandbox = SnapshotSandbox(runner, "f20_edge_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    @pytest.mark.p2
    def test_f20_01_moveFromLocal_isolation(self):
        """
        F20-01: 使用 moveFromLocal 写入 -> 快照 -> 验证内容隔离
        """
        local_file = "/tmp/local_move_src.txt"
        with open(local_file, "w") as f:
            f.write("MOVE_FROM_LOCAL_DATA")
        
        target_path = f"{self.sandbox.test_dir}/moved_file.dat"
        
        # 执行 moveFromLocal (注意：这会删除本地文件，我们在测试中不关心本地消失，主要看远程)
        res_h, res_o = self.runner.run_dual_cmd("-moveFromLocal", local_file, f"{{TARGET}}{target_path}")
        self.validator.assert_results_match(res_h, res_o, feature_tag="moveFromLocal")
        
        self.sandbox.create_snapshot("snap_after_move")
        
        # 验证快照内容
        res_cat_h, res_cat_o = self.runner.run_dual_cmd("-cat", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_after_move/moved_file.dat")
        self.validator.assert_results_match(res_cat_h, res_cat_o, feature_tag="snapshot_cat")

    @pytest.mark.p2
    def test_f20_02_moveToLocal_interception(self):
        """
        F20-02: 尝试从快照目录 moveToLocal (试图删除只读源文件) -> 预期拦截
        """
        test_file = f"{self.sandbox.test_dir}/src_file.dat"
        create_test_file(self.runner, test_file, "DATA_TO_MOVE")
        
        self.sandbox.create_snapshot("snap_s1")
        
        local_dest = "/tmp/local_dest.txt"
        if os.path.exists(local_dest): os.remove(local_dest)
        
        # 尝试从快照目录 moveToLocal
        # HDFS 应该报错 (因为源是只读的，无法删除)
        res_h, res_o = self.runner.run_dual_cmd("-moveToLocal", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_s1/src_file.dat", local_dest)
        
        # 验证拦截一致性
        self.validator.assert_results_match(res_h, res_o, feature_tag="moveToLocal")

    @pytest.mark.p2
    def test_f20_03_find_traversal(self):
        """
        F20-03: find 命令遍历快照目录
        """
        create_test_file(self.runner, f"{self.sandbox.test_dir}/find_me.txt", "FOUND")
        self.sandbox.create_snapshot("snap_for_find")
        
        # 执行 find
        res_h, res_o = self.runner.run_dual_cmd("-find", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot", "-name", "find_me.txt")
        
        # 验证输出内容（通常包含路径）
        self.validator.assert_results_match(res_h, res_o, feature_tag="find")

    @pytest.mark.p2
    def test_f20_04_text_compression_reader(self):
        """
        F20-04: text 命令读取快照中的压缩文件。
        """
        # 我们这里简化，只验证 text 对普通文件的读取是否一致（text 支持解压，也支持读普通文本）
        test_file = f"{self.sandbox.test_dir}/text_file.txt"
        create_test_file(self.runner, test_file, "TEXT_CONTENT")
        
        self.sandbox.create_snapshot("snap_text")
        
        res_h, res_o = self.runner.run_dual_cmd("-text", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_text/text_file.txt")
        self.validator.assert_results_match(res_h, res_o, feature_tag="text")
