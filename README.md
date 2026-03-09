# OBSA Snapshot Parity Tester (OBSA 快照回归比对测试框架)

本工具是一个**自动化、端到端、强一致性验证**框架，专门用于全面验证基于 Hadoop 的云存储插件（如华为云 OBSA）与原生 HDFS 在快照（Snapshot）语义上的一致性。

---

## 🌟 一、 核心亮点

- **双端比对引擎 (Dual-Run Parity)**：同一指令同步下发至 HDFS 与 OBSA，深度比对 `stdout`/`stderr` 指纹及 `returncode`。
- **阶梯式测试策略**：内置 72 个覆盖全场景的测试用例（`tests/test_strategy.md`），从基础 Lifecycle 到极端的越权拦截测试。
- **已验证的 P0 用例集**：预置 24 个重点核心用例，并已在 Mock 模式下实现 **100% 通过率**。
- **无缝离线部署**：内建 `setup_offline_env.sh` 工具，支持“外网一键打包、内网静默环境恢复”。
- **灵活执行模式**：支持 `mock_obsa_mode`。在无真实 OBS 环境时，可通过本地 HDFS 沙箱模拟 OBSA 行为进行逻辑预演。

---

## 📂 二、 工程结构

```text
snapshot_tester/
├── config.yml              # 全局配置（端点映射、Mock 模式开关、数据拓扑）
├── dual_runner.py          # 核心驱动：双端命令下发与一致性仲裁逻辑
├── setup_offline_env.sh    # 环境保障：离线包封装与本地 venv 自动化部署
├── logs/                   # 运行日志：自动按时间戳分文件记录 (run_YYYY-MM-DD_HH-mm-ss.log)
├── tests/                  # 测试套件
│   ├── test_strategy.md    # 测试白皮书：定义了 F1-F14 章节的全部用例
│   ├── conftest.py         # Pytest 配置：内嵌结果捕获 Hook 与全局 Fixture
│   ├── test_f1_lifecycle.py       # P0: 快照全生命周期功能验证
│   └── ...                 # F1-F14 专项测试文件
└── host_venv/              # 隔离执行环境 (由 setup_offline_env.sh 维护)
```

---

## 🚀 三、 快速上手 (外网 -> 内网一键迁移)

本框架支持在**离线环境**下的一键式部署。

### 1. 外网打包 (Pack)
在有外网的开发机上执行：
```bash
bash ./setup_offline_env.sh pack
```
产物：项目上一级目录下的 `obsa_test_framework_offline.tar.gz`。

### 2. 内网安装 (Install)
将压缩包上传至内网节点并解压：
```bash
tar -xzvf obsa_test_framework_offline.tar.gz
cd snapshot_tester
bash ./setup_offline_env.sh install
source venv/bin/activate
```

---

## 🧪 四、 测试执行模式

通过修改 `config.yml` 中的 `mock_obsa_mode` 切换运行模式：

| 模式 | 配置 | 说明 |
| :--- | :--- | :--- |
| **Mock 模式** | `true` | **推荐模式**。将 OBS 命令重定向至本地 HDFS 的 Mock 路径。适合在没有 OBSA 插件的环境下验证快照逻辑与 Hadoop 命令的一致性。 |
| **实测模式** | `false` | 使用真实的 `obs://` 路径和 OBSA 插件进行端到端生产级验证。 |

---

## 📂 五、 测试执行与用例体系

用例定义于 `tests/` 目录，遵循 `tests/test_strategy.md` 规划：

- **命名规范**：`test_f{章节}_{功能点}.py`
- **核心用例库**：
    - `test_f1_lifecycle.py`: 快照全生命周期主流程 (P0)
    - `test_f2_snapshot_diff.py`: `snapshotDiff` 差异标记 (+/-/M/R) 深度验证 (P0)
    - `test_f11_readonly_block.py`: `.snapshot` 隐藏目录全指令写拦截验证 (P0)
    - `test_f12_multi_snap_chain.py`: 多快照时序链与删除中间态验证 (P2)
    - `...`: 涵盖权限、副本、深层嵌套等 F1-F14 全场景。

### 1. 执行命令示例

```bash
# A. 执行最核心的 P0 用例 (快速冒烟)
pytest -m p0 -v

# B. 执行 P1 级验证用例
pytest -m p1 -v

# C. 执行全量 P2 高级/场景化用例
pytest -m p2 -v

# D. 执行具有p1标签的用例，并且生成可视化 HTML 报表 (推荐)
pytest -m p1 --html=report.html --self-contained-html

# E. 执行指定的测试文件
pytest tests/test_f10_queries.py -v

# F. 执行特定测试文件中，标签为p0的测试用例
pytest tests/test_f1_lifecycle.py -m p0 -v
```

### 2. 日志追踪
执行期间，详细的步骤信息与双端比对指纹会实时同步至 `logs/` 目录。
文件名包含精确时间戳，例如 `run_2026-03-08_20-10-31.log`。你可以通过搜索 `TEST RESULT` 快速定位用例状态。

---

## 🔍 六、 核心工作流：SnapshotDiff 验证

本框架最强大的功能之一是验证差异报告的一致性。测试会验证以下标记的字节级对齐：

- **`+` (Added)**: 活跃目录新增的文件/目录。
- **`-` (Deleted)**: 活跃目录中被删除但快照中仍存的文件/目录。
- **`M` (Modified)**: 内容发生变更、Metadata 变化或执行过 `truncate`。
- **`R` (Renamed)**: 同目录下发生的移动/重命名行为。

---

## 🛠️ 七、 常见问题排查 (Troubleshooting)

### 1. Docker 环境下 appendToFile 报错
**现象**：`java.io.IOException: Failed to replace a bad datanode...`
**原因**：在单 DataNode 的 Mock/Docker 环境中，HDFS 默认流水线策略导致追加写入失败。
**解决**：本框架已自动注入 `replace-datanode-on-failure.enable=false` 配置。

### 2. snapshotDiff 命令不可用
**提示**：`-snapshotDiff: Unknown command`
**原因**：`snapshotDiff` 是 `hdfs` 的顶层命令，而非 `dfs` 的子命令。
- 正确：`hdfs snapshotDiff <path> <v1> <v2>`
- 错误：`hdfs dfs -snapshotDiff ...`
**解决**: 框架底层 `dual_runner.py` 已自动处理此差异。

---

## 📝 八、 开发与扩展

若需新增测试用例，请参考 `tests/test_helpers.py`：
- 使用 `SnapshotSandbox` 管理沙箱环境。
- 使用 `create_test_file` 替代传统的本地 `put` 流程，确保容器内外路径映射正确（宿主机目录挂载于容器 `/obsa_workspace`）。

---

*Better snapshot support, reliable cloud storage plugin.*
