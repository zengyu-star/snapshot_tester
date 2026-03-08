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

## 🚀 三、 快速上手

### 1. 环境准备 (离线/内网)
如果是首次在隔离环境运行：
```bash
bash ./setup_offline_env.sh install
source venv/bin/activate
```

### 2. 配置准星 (`config.yml`)
确保 `mock_obsa_mode` 按需配置。Mock 模式（开启时）会自动将 OBS 路径映射至 HDFS 的测试子目录：
```yaml
global:
  mock_obsa_mode: true  # 建议先在 Mock 模式验证逻辑，再切真机 OBS
```

### 3. 执行测试

框架集成了 `pytest` 标记系统，支持按优先级（Priority）精细化执行：

#### A. 运行指定优先级的用例
```bash
# 执行最核心的 P0 用例 (快速冒烟)
pytest -m p0 -v

# 执行 P1 级验证用例
pytest -m p1 -v

# 执行全量 P2 高级/场景化用例
pytest -m p2 -v
```

#### B. 生成可视化 HTML 报表
执行完毕后可直接查看图形化测试结果，包含成功率统计与耗时分析：
```bash
pytest -m p1 --html=report.html --self-contained-html
```

#### C. 运行特定专项测试
```bash
# 仅运行“只读性拦截”相关测试
pytest tests/test_f11_readonly_block.py -v -s
```

#### D. 日志追踪
执行期间，详细的步骤信息与双端比对指纹会实时同步至 `logs/` 目录。
文件名包含精确时间戳，例如 `run_2026-03-08_20-10-31.log`。你可以通过搜索 `TEST RESULT` 快速定位用例状态。

---

## 🔍 四、 核心工作流：SnapshotDiff 验证

本框架最强大的功能之一是验证差异报告的一致性。测试会模拟如下变异序列：
1. **Snap A** -> **新增文件/删除文件/追加数据/重命名** -> **Snap B**
2. 调用 `hdfs snapshotDiff` 获取增量报告。
3. 比对 HDFS 原生标记与 OBSA 插件标记（`+`/`-`/`M`/`R`）是否字节级对齐。

---

## 🛠️ 五、 常见问题解答 (FAQ)

- **为什么 appendToFile 会失败？**
  在单 DataNode 的 Docker Mock 环境中，这是常见的 HDFS 默认流水线冲突。
  **解决**：本框架已自动注入 `replace-datanode-on-failure.enable=false` 配置。
- **如何新增自定义用例？**
  在 `tests/` 下参考 `test_f11_readonly_block.py`，使用 `SnapshotSandbox` 快速构建您的测试沙箱。

---

*Better snapshot support, reliable cloud storage plugin.*
