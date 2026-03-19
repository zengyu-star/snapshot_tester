import pytest
import logging
import time
from test_helpers import SnapshotSandbox, create_test_file

logger = logging.getLogger("TestTrash")

class TestTrashInteraction:
    """
    F19: 快照 × 回收站机制交互 (Trash Semantics)
    验证 HDFS 回收站交互逻辑在快照场景下的一致性。
    """

    @pytest.fixture(autouse=True)
    def setup_teardown(self, runner, validator):
        self.runner = runner
        self.validator = validator
        self.sandbox = SnapshotSandbox(runner, "f19_trash_test")
        self.sandbox.setup()
        self.sandbox.allow_snapshot()
        yield
        self.sandbox.teardown()

    @pytest.mark.p1
    def test_f19_01_rm_to_trash_isolation(self):
        """
        F19-01: 赋权 -> 快照A -> rm(不带-skipTrash) -> 验证文件进入.Trash -> ls .snapshot/A
        目标：验证删除到回收站后，快照依然指向原始块，且活跃目录中回收站链路正常。
        """
        test_file = f"{self.sandbox.test_dir}/trash_file.dat"
        create_test_file(self.runner, test_file, content="TRASH_CONTENT")
        
        self.sandbox.create_snapshot("snap_before_rm")
        
        # 执行删除（不带 -skipTrash，通常 HDFS 会进入回收站，若配置开启）
        # 注意：Mock 模式下 Docker 默认可能没开 Trash，或者路径在 /user/root/.Trash
        res_h, res_o = self.runner.run_dual_cmd("-rm", f"{{TARGET}}{test_file}")
        
        # 即使进入回收站，也要求基本一致性
        self.validator.assert_results_match(res_h, res_o, feature_tag="rm")
        
        # 验证快照不受影响
        res_cat_h, res_cat_o = self.runner.run_dual_cmd("-cat", f"{{TARGET}}{self.sandbox.test_dir}/.snapshot/snap_before_rm/trash_file.dat")
        self.validator.assert_results_match(res_cat_h, res_cat_o, feature_tag="snapshot_cat")

    @pytest.mark.p1
    def test_f19_02_expunge_interaction(self):
        """
        F19-02: 验证 expunge 命令。
        OBSA 通常不支持 expunge（因为可能没有 HDFS 风格的回收站管理）。
        """
        # expunge 是 dfs 级命令，但在 HDFS 中可能需要定期或者手动执行
        # 使用 -fs 显式指定目标文件系统，确保 expunge 被发送到正确的 OBS/Mock 挂载点
        # run_dual_cmd 会将 {TARGET} 替换为对应的 hdfs_base 和 obs_base
        res_h, res_o = self.runner.run_dual_cmd("-fs", "{TARGET}", "-expunge")
        
        # 验证拦截逻辑
        self.validator.assert_results_match(res_h, res_o, feature_tag="expunge")