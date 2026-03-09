#!/bin/bash
# ==========================================
# OBSA 测试框架离线环境构建与安装脚本
# 用法:
#   外网打包: ./setup_offline_env.sh pack
#   内网安装: ./setup_offline_env.sh install
# ==========================================

set -e # 遇到错误立即退出

MODE=$1
PROJECT_DIR=$(pwd)
PACKAGE_DIR="${PROJECT_DIR}/offline_packages"
VENV_DIR="${PROJECT_DIR}/venv"
ARCHIVE_NAME="obsa_test_framework_offline.tar.gz"

if [ "$MODE" == "pack" ]; then
    echo ">>> [外网模式] 开始下载离线依赖包..."
    
    # 清理历史残留
    rm -rf "${PACKAGE_DIR}"
    mkdir -p "${PACKAGE_DIR}"
    
    # 利用 pip download 仅下载不安装（注意：外网开发机的 Python 版本与系统架构需与内网 Hadoop 节点尽量保持一致）
    ./host_venv/bin/pip download -r requirements.txt -d "${PACKAGE_DIR}"
    
    echo ">>> 正在更新 config.yml 为内网环境默认配置..."
    # 使用 sed 更新 HDFS 和 OBS 管理 URI
    sed -i 's|hdfs_base_uri:.*|hdfs_base_uri: "hdfs://namenode:8020/native_obsa_test"|' config.yml
    sed -i 's|obs_admin_uri:.*|obs_admin_uri: "hdfs://namenode:8020/hadoop/obsa_test_workspace"|' config.yml

    echo ">>> 依赖下载完成。正在打包整个测试工程..."
    
    # 获取父目录绝对路径
    PARENT_DIR=$(dirname "$PROJECT_DIR")
    OUTPUT_ARCHIVE="${PARENT_DIR}/${ARCHIVE_NAME}"
    
    cd "${PARENT_DIR}"
    # 将源码、配置文件、依赖包打包在一起（排除不必要的体积消耗）
    tar -czvf "${OUTPUT_ARCHIVE}" \
        --exclude="host_venv" \
        --exclude="venv" \
        --exclude="logs" \
        --exclude=".git" \
        --exclude=".agents" \
        --exclude="__pycache__" \
        --exclude=".pytest_cache" \
        --exclude="hdfs" \
        $(basename "${PROJECT_DIR}")
        
    echo ">>> [外网模式完成] 离线包已生成: ${OUTPUT_ARCHIVE}"
    echo ">>> 请将该压缩包拷贝至内网 Hadoop 节点执行 install 模式。"

elif [ "$MODE" == "install" ]; then
    echo ">>> [内网模式] 开始离线部署测试环境..."
    
    if [ ! -d "${PACKAGE_DIR}" ]; then
        echo "错误: 找不到 offline_packages 目录，请确保已正确解压全量包！"
        exit 1
    fi

    echo ">>> 1. 创建 Python 独立虚拟环境 (沙箱隔离)..."
    python3 -m venv "${VENV_DIR}" || python3.7 -m venv "${VENV_DIR}"
    source "${VENV_DIR}/bin/activate"
    
    # 升级基础组件（使用离线包里的包）
    pip install --no-index --find-links="${PACKAGE_DIR}" pip setuptools wheel
    
    echo ">>> 2. 从本地缓存安装依赖包..."
    pip install --no-index --find-links="${PACKAGE_DIR}" -r requirements.txt
    
    echo ">>> [内网模式完成] 测试环境已就绪！"
    echo ">>> 启动测试前，请先激活环境："
    echo ">>> source ${VENV_DIR}/bin/activate"
    echo ">>> 然后执行: ./host_venv/bin/pytest tests/ -v -s"

else
    echo "错误: 未知模式 '$MODE'"
    echo "用法: $0 [pack|install]"
    exit 1
fi
