#!/bin/bash
# ================================================
#  一键部署: HDFS上传 + Flink集群 + 提交Job + 可视化
#  用法: bash setup_hdfs_flink.sh
# ================================================

set -e

PROJECT_DIR="/home/mushuting/vgsales-flink"
FLINK_VERSION="1.16.2"
HADOOP_VERSION="3.2.2"
FLINK_HOME="/home/mushuting/flink-${FLINK_VERSION}"

echo "================================================"
echo "  Flink + HDFS 一键部署脚本"
echo "================================================"

# ========== Step 0: 检查/安装 Flink ==========
echo ""
echo "[0/6] 检查 Flink 环境..."
if [ ! -d "$FLINK_HOME" ]; then
    echo "  下载 Flink ${FLINK_VERSION}..."
    cd /home/mushuting
    wget -q --show-progress https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz
    tar -xzf flink-${FLINK_VERSION}-bin-scala_2.12.tgz
    rm -f flink-${FLINK_VERSION}-bin-scala_2.12.tgz
    echo "  Flink 安装完成"
else
    echo "  Flink 已存在: $FLINK_HOME"
fi

# ========== Step 1: 编译打包 ==========
echo ""
echo "[1/6] 编译打包项目..."
cd "$PROJECT_DIR"
# 先同步 pom.xml 和 Java 文件
source ~/spark/venv/bin/activate
MAVEN_OPTS="-Xmx2048m" mvn clean package -DskipTests -q
echo "  打包完成"
ls -lh target/vgsales-flink-1.0-all.jar

# ========== Step 2: 检查/启动 HDFS ==========
echo ""
echo "[2/6] 检查 HDFS 状态..."
if command -v hdfs &> /dev/null; then
    if ! hdfs dfs -test -d /user/mushuting 2>/dev/null; then
        echo "  创建HDFS用户目录..."
        hdfs dfs -mkdir -p /user/mushuting/input
        hdfs dfs -mkdir -p /user/mushuting/output
    fi
    echo "  HDFS 可用"
else
    echo "  HDFS 未安装，尝试使用本地模式代替..."
    echo "  请确保 Hadoop 已配置并启动"
fi

# ========== Step 3: 上传数据到 HDFS ==========
echo ""
echo "[3/6] 上传数据到 HDFS..."
if command -v hdfs &> /dev/null; then
    hdfs dfs -rm -f /user/mushuting/input/vgchartz-2024.csv 2>/dev/null || true
    hdfs dfs -put data/vgchartz-2024.csv /user/mushuting/input/
    echo "  数据已上传到 HDFS: /user/mushuting/input/vgchartz-2024.csv"
    echo "  验证:"
    hdfs dfs -ls /user/mushuting/input/
else
    echo "  跳过HDFS上传"
fi

# ========== Step 4: 清空旧输出 ==========
echo ""
echo "[4/6] 清空旧输出..."
rm -f output/flink_*.txt output/0*.txt output/10_*.txt
if command -v hdfs &> /dev/null; then
    hdfs dfs -rm -r -f /user/mushuting/output/flink_* 2>/dev/null || true
fi
echo "  完成"

# ========== Step 5: 启动Flink集群 ==========
echo ""
echo "[5/6] 启动 Flink 集群..."
cd "$FLINK_HOME"
# 停止旧集群
./bin/stop-cluster.sh 2>/dev/null || true
sleep 2
# 启动
./bin/start-cluster.sh
sleep 3
echo "  Flink 集群已启动"
echo ""
echo "  --------------------------------------------"
echo "  Web UI: http://node3:8081"
echo "  JobManager: node3:6123"
echo "  --------------------------------------------"

# ========== Step 6: 提交Flink Job ==========
echo ""
echo "[6/6] 提交 Flink Job..."

# 尝试HDFS模式，失败则用本地文件模式
if command -v hdfs &> /dev/null && hdfs dfs -test -e /user/mushuting/input/vgchartz-2024.csv 2>/dev/null; then
    echo "  使用 HDFS 模式提交..."
    ./bin/flink run -c vgsales.VideoGameSalesFlinkAnalysisHDFS \
        "$PROJECT_DIR/target/vgsales-flink-1.0-all.jar" \
        "hdfs://node3:9000/user/mushuting/input/vgchartz-2024.csv" \
        "hdfs://node3:9000/user/mushuting/output/flink_"
else
    echo "  使用本地文件模式提交..."
    # 回退：用本地模式但通过 flink run 提交（仍能看到 Web UI）
    ./bin/flink run -c vgsales.VideoGameSalesFlinkAnalysisLocal \
        "$PROJECT_DIR/target/vgsales-flink-1.0-all.jar" \
        "file://$PROJECT_DIR/data/vgchartz-2024.csv" \
        "file://$PROJECT_DIR/output/flink_"
fi

echo ""
echo "  Job 已提交！查看 Web UI: http://node3:8081"

# ========== 等待Job完成 ==========
echo ""
echo "  等待 Job 执行完成（约1-2分钟）..."
sleep 60

# ========== 从HDFS下载结果（如果用HDFS） ==========
echo ""
echo "  下载结果文件..."
if command -v hdfs &> /dev/null; then
    cd "$PROJECT_DIR/output"
    hdfs dfs -get /user/mushuting/output/flink_* . 2>/dev/null || true
    # 重命名
    for f in flink_*.txt; do
        if [ -f "$f" ]; then cp "$f" "${f#flink_}"; fi
    done
    if [ -f "10_yearly_best_game.txt" ] && [ ! -f "09_yearly_best_game.txt" ]; then
        cp "10_yearly_best_game.txt" "09_yearly_best_game.txt"
    fi
    cd "$PROJECT_DIR"
fi

# ========== 生成可视化 ==========
echo ""
echo "  生成可视化图表..."
source ~/spark/venv/bin/activate
cd "$PROJECT_DIR/visualization"
python visualize_java.py
cd "$PROJECT_DIR"

# ========== 完成 ==========
echo ""
echo "================================================"
echo "  全部完成！"
echo ""
echo "  Flink Web UI: http://node3:8081"
echo "  图表目录: $PROJECT_DIR/visualization/"
echo "  输出数据: $PROJECT_DIR/output/"
echo "  HDFS数据: hdfs://node3:9000/user/mushuting/"
echo "================================================"
echo ""
echo "图表列表:"
ls -la "$PROJECT_DIR/visualization/java_*.png" 2>/dev/null || echo "  (等待Job完成后生成)"
echo ""
echo "  停止 Flink 集群: cd $FLINK_HOME && ./bin/stop-cluster.sh"
