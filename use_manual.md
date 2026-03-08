# OBSA 测试框架离线操作指南

本指南用于说明如何在**无外网访问权限**的测试集群节点上，通过在一台有外网的跳板机（或开发机）上打包完整环境，随后将其迁移至内网进行一键化安装运行的完整工作流。

## 一、 外网机：环境打包 (Pack)

在拥有互联网访问能力的机器（如您的开发电脑）上下载依赖并将整个测试沙箱组装为压缩包。

> [!WARNING]
> **架构匹配**：请尽量确保您使用的外网打包机器与内网目标机器运行相同体系结构（如都是 `x86_64` 的 Linux）以及接近的 Python 3.7+ 版本，以防某些底层被编译的库迁移后出现不兼容。

1. **进入工作空间**
   ```bash
   cd /path/to/snapshot_tester
   ```

2. **执行打包指令**
   ```bash
   ./setup_offline_env.sh pack
   ```

3. **获取输出产物**
   脚本会自动下载执行所需的 Python 包暂存至 `offline_packages/` 目录，随后排除掉本地的无用文件（如历史 `logs/`, `.git/`, `host_venv/` 等），在**该工程目录的父目录**下生成名为 `obsa_test_framework_offline.tar.gz` 的压缩文件。

---

## 二、 物理迁移

使用优盘、SCP、SFTP 或其他方式，将外部获取到的 `obsa_test_framework_offline.tar.gz` 传输至内网集群的目标 Hadoop 客户端节点上的任意工作目录。

---

## 三、 内网机：离线安装 (Install)

在目标机器上解压并依赖本地缓存安装所有包。

1. **解压工程文件**
   ```bash
   tar -xzvf obsa_test_framework_offline.tar.gz
   cd snapshot_tester
   ```

2. **执行离线化安装指令**
   ```bash
   ./setup_offline_env.sh install
   ```
   *说明：该脚本会自动在此目录下创建一个隔离的 `venv/` 虚拟环境，并断开网络索引，完全使用包内的轮子进行本地安装。*

---

## 四、 运行验收

环境配置完成后，即可启动常规测试流水线。

1. **激活虚拟环境**
   ```bash
   source venv/bin/activate
   ```

2. **确认配置（可选）**
   建议检查 `config.yml` 内关于 `hadoop_namenode` 等核心地址是否在目标节点的网段连通范畴内。如果有特殊 Mock 需求，可在其中修改 `mock_obsa_mode: true`。

3. **执行测试框架**
   使用刚刚安装的 pytest 执行测试脚本集：
   ```bash
   pytest tests/ -v -s
   ```
   *注意：无需指定具体的 Python 文件路径，系统会自动检索 `tests/` 子目录并开始运行，测试全过程输出以及高亮结果会呈现在屏幕上。执行的历史记录将保存在项目内的 `logs/` 文件夹下。*
