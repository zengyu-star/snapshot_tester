# OBSA 快照测试框架使用手册 (OBSA Snapshot Tester)

本手册详细说明 OBSA 快照测试框架的部署、运行、用例组织及结果分析。

---

## 🚀 一、 快速开始 (外网 -> 内网一键迁移)

本框架支持在**离线环境**下的一键式部署。

### 1. 外网打包 (Pack)
在有外网的开发机上执行：
```bash
./setup_offline_env.sh pack
```
产物：`../obsa_test_framework_offline.tar.gz`

### 2. 内网安装 (Install)
将压缩包上传至内网节点并解压：
```bash
tar -xzvf obsa_test_framework_offline.tar.gz
cd snapshot_tester
./setup_offline_env.sh install
```

---

## 🧪 二、 测试执行模式

通过修改 `config.yml` 中的 `mock_obsa_mode` 切换运行模式：

| 模式 | 配置 | 说明 |
| :--- | :--- | :--- |
| **Mock 模式** | `true` | **推荐模式**。将 OBS 命令重定向至本地 HDFS 的 Mock 路径。适合在没有 OBSA 插件的环境下验证快照逻辑与 Hadoop 命令的一致性。 |
| **实测模式** | `false` | 使用真实的 `obs://` 路径和 OBSA 插件进行端到端生产级验证。 |

---

## 📂 三、 测试用例体系

用例定义于 `tests/` 目录，遵循 `tests/test_strategy.md` 规划：

- **命名规范**：`test_f{章节}_{功能点}.py`
- **核心章节**：
    - `test_f1_lifecycle.py`: 快照全生命周期主流程 (P0)
    - `test_f2_snapshot_diff.py`: `snapshotDiff` 差异标记 (+/-/M/R) 深度验证 (P0)
    - `test_f3_write_isolation.py`: 写入类命令 (put/append) 对快照的隔离性验证 (P0)
    - `test_f5_rm_mv.py`: 删除与移动操作下的快照持久化 (P0)
    - `test_f11_readonly_block.py`: `.snapshot` 隐藏目录全指令写拦截验证 (P0)

**执行命令**：
```bash
source venv/bin/activate
pytest tests/ -v -s
```

---

## 📊 四、 结果解读与差异分析 (snapshotDiff)

本框架核心在于验证 OBSA 提供的差异报告是否与原生 HDFS 分毫不差：

- **`+` (Added)**: 活跃目录新增的文件/目录。
- **`-` (Deleted)**: 活跃目录中被删除但快照中仍存的文件/目录。
- **`M` (Modified)**: 内容发生变更、Metadata 变化或执行过 `truncate`。
- **`R` (Renamed)**: 同目录下发生的移动/重命名行为。

---

## 🛠️ 五、 常见问题排查 (Troubleshooting)

### 1. Docker 环境下 appendToFile 报错
**现象**：`java.io.IOException: Failed to replace a bad datanode...`
**原因**：在单 DataNode 的 Mock/Docker 环境中，HDFS 默认流水线策略导致追加写入失败。
**解决**：
框架已在 namenode 容器执行配置注入，或手动确保 `hdfs-site.xml` 包含：
```xml
<property>
  <name>dfs.client.block.write.replace-datanode-on-failure.enable</name>
  <value>false</value>
</property>
```

### 2. snapshotDiff 命令不可用
**提示**：`-snapshotDiff: Unknown command`
**解决**：
注意 `snapshotDiff` 是 `hdfs` 的顶层命令，而非 `dfs` 的子命令。
- 正确：`hdfs snapshotDiff <path> <v1> <v2>`
- 错误：`hdfs dfs -snapshotDiff ...`
框架底层 `dual_runner.py` 已自动通过 `run_dual_hdfs_cmd` 处理此差异。

---

## 📝 六、 开发与扩展

若需新增测试用例，请参考 `tests/test_helpers.py`：
- 使用 `SnapshotSandbox` 管理沙箱环境。
- 使用 `create_test_file` 替代传统的本地 `put` 流程，确保容器内外路径映射正确（宿主机目录挂载于容器 `/obsa_workspace`）。
