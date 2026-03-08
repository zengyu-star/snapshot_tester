# OBSA Snapshot Parity Tester (OBSA 快照回归比对测试框架)

本工具旨在提供一个**自动化、强比对、跨环境**的混合测试框架，用于全面验证基于 Hadoop 的云存储插件（如华为云 OBSA）对高版本 HDFS 快照（Snapshot）语意的支持情况。

通过“双端沙箱比对”（Dual-Run Parity Check）机制，框架能在毫秒级同时对 HDFS 原生集群和对象存储（OBS）发出一模一样的各种文件突变操作，并采用严苛的 `stdout`、`stderr` 和 `returncode` 三合一探针，实时判断由外部插件产生的行为是否与原生 HDFS 百分百一致（包含防呆拦截、权限拒绝等边界测试）。

---

## 一、 核心特性
- **高度还原的快照生命周期测试**：涵盖从建树、存证、增删改查到隐式路径只读性等一系列标准行为的安全验证。
- **并发环境的数据篡改（DataMutator）**：使用内置的线程池机制并发构造海量测试集。
- **Python 3.7+ 现代架构支持**：支持最新的高并发库和断言捕捉，兼容企业级离线系统集群。
- **智能一键离线部署打包体系**：内建外网拉取、内网静默恢复环境的全套依赖转移脚本，无缝兼容断网 Hadoop。
- **安全的 Mock 沙箱兜底**：即使没有准备好最终的云上存储环境和 AK/SK，也能一键路由至本地集群的安全沙箱中完成预检通过。

---

## 二、 工程结构

```text
snapshot_tester/
├── config.yml                      # 【核心】全局配置、数据拓扑模型与集群路由白名单
├── test_snapshot_lifecycle.py      # 【核心】符合 Pytest 标准的端对端快照全量测试用例集
├── dual_runner.py                  # 双端运行引线，同时执行命令并执行差异化仲裁拦截器
├── data_mutator.py                 # 多线程沙箱并发注射器，模拟复杂的 HDFS 数据突变事件
├── requirements.txt                # 工程依赖声明文件 (自动锁定适合内网大数据的兼容版本)
├── setup_offline_env.sh            # 一键外网封装/一键内网部署的环境保障利器
└── docker-compose.yml              # (可选) 用于开发者在本机或缺乏 Hadoop 时的轻量体验环境
```

---

## 三、 使用指南：从外网到内网的流转全过程

如果您处于包含“外网打包机”与“内网/生产 Hadoop 集群”的典型交付场景，请严格遵循以下步骤操作：

### 阶段 1：在外网设备打包出无依赖的纯净包

1. **拉取工程代码到有外网的跳板机**
2. **执行打包指令：**
   ```bash
   cd snapshot_tester
   bash ./setup_offline_env.sh pack
   ```
   > 脚本将自动抽取 `requirements.txt` 中所有的模块轮子包（如 pytest 及其配套插件），并统一压缩上卷。
3. **获取产物**：该命令执行完毕后，在项目 **上一级目录** 会生成名为 `obsa_test_framework_offline.tar.gz` 的包裹。请将其拷贝至隔离网络内。

---

### 阶段 2：在内网 Hadoop/生产机进行静默部署

1. **将拷贝进来的产物在内网任意带 HDFS 客户端的部署机解压：**
   ```bash
   tar -xzf obsa_test_framework_offline.tar.gz
   cd snapshot_tester
   ```
2. **执行本地环境一键复原：**
   ```bash
   bash ./setup_offline_env.sh install
   ```
   > 此时它将断网利用内置的离线包为您激活独立的本地 `python3` / `python3.7` 沙盒，将这台主机的库隔离保护。

---

### 阶段 3：微调测试准星以指向核心标靶 (`config.yml`)

在使用前务必检查并修改配置文件，以真正触达待测插件：
```yaml
global:
  # 改为 false 关掉沙盒演练模式，正式开始真刀真枪攻击您的 OBS
  mock_obsa_mode: false

cluster_env:
  # 务必保证这里的 hdfs 和 obs 的 Scheme 地址在这台主机的 core-site.xml 中可用
  hadoop_namenode: "hdfs://[您的_NameNode_真实IP或域名]:8020"
  obs_bucket: "your-testing-bucket"
```

---

### 阶段 4：执行大扫除级别的探针测试

激活我们刚刚部署好的内网环境后，敲下执行按键：

```bash
source venv/bin/activate
pytest test_snapshot_lifecycle.py -v -s
```

#### 你会在此刻看到什么？
框架将如同自动驾驶一般跑完以下关键哨所：
1. **并发注入造数** -> 各端并发涌入数百个加密分块；
2. **静态快照锚定** -> 提取当前时刻的快照指纹；
3. **数据黑客式篡改** -> 删除原始文件并在尾部恶意拼接；
4. **回溯验证** -> 从 `.snapshot` 时空隧道穿越回去核对数据尸体；
5. **防御击破试图** -> （高亮！）尝试以写模式强行突破隐藏只读 `.snapshot` 区域。
   > 如果在这里控制台没有爆红，反而成功拿到了**权限拒绝 Exception**（两者报错一模一样），那恭喜你，插件的安全生命周期已经被完全打通并通过了审计！

---

## 四、 常见踩坑问题

#### 1. "NameError: name 'subprocess' is not defined" 等类型语法报错？
本工程由于已全面支持企业常见的大数据集群依赖生态（CentOS 8 / RedHat），对 Python 环境的依赖底线是 **Python 3.7+** 。请确保测试执行机带有 `python3` 或 `python3.7` 解析器。如果您的内网集群实在老旧到只能用 Python 3.5 以下版本，可能会存在少量 f-string 不识别问题。

#### 2. 在真机跑测试报错：“No FileSystem for scheme: obs” ？
这不是本工程的 Bug。框架是基于纯原生 `HDFS API` 及底层 `RPC` 设计的。这意味着：在您跑 `pytest` 的这台节点上的 `$HADOOP_HOME/etc/hadoop/core-site.xml` 里，务必已经挂挂载好了华为云 `obs-filesystem` 的 `.jar` 插件包并配置了密钥组合（AK/SK）。最简单的自验手段就是：测试前在这里终端直接跑一把 `hdfs dfs -ls obs://xxx/` 看看通不通！
