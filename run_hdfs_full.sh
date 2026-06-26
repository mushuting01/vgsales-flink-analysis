#!/bin/bash
# ================================================
#  HDFS版一键运行脚本 - 启动HDFS/Flink -> 分析 -> 可视化
#  用法: bash run_hdfs_full.sh
# ================================================

set -e

PROJECT_DIR="/home/mushuting/vgsales-flink"
FLINK_HOME="/home/mushuting/flink-1.17.1"
HADOOP_HOME="/export/server/hadoop"

echo "================================================"
echo "  Flink + HDFS 游戏销售数据分析 - 一键运行"
echo "================================================"

# ====== Step 1: 启动 HDFS ======
echo ""
echo "[1/4] 启动 HDFS..."
start-dfs.sh
sleep 2
echo "  HDFS 已启动"
echo "  NameNode Web UI: http://node3:9870"

# ====== Step 2: 设置环境变量 + 启动 Flink ======
echo ""
echo "[2/4] 设置环境变量 + 启动 Flink..."
export HADOOP_HOME="$HADOOP_HOME"
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)

cd "$FLINK_HOME"
./bin/stop-cluster.sh 2>/dev/null || true
sleep 1
./bin/start-cluster.sh
sleep 2
echo "  Flink 已启动"
echo "  Flink Web UI: http://node3:8081"

# ====== Step 3: 删除旧输出 + 提交 Flink Job ======
echo ""
echo "[3/4] 清空旧输出 + 提交 Flink Job..."
hdfs dfs -rm -r -f /user/mushuting/output/flink_* 2>/dev/null || true

./bin/flink run -c vgsales.VideoGameSalesFlinkAnalysisHDFS \
    "$PROJECT_DIR/target/vgsales-flink-1.0-all.jar" \
    "hdfs://localhost:9000/user/mushuting/input/vgchartz-2024.csv" \
    "hdfs://localhost:9000/user/mushuting/output/flink_"

echo "  Job 已提交完成！"

# ====== Step 4: 下载结果 + 生成图表 ======
echo ""
echo "[4/4] 下载结果 + 生成可视化图表..."

# 下载 HDFS 输出到本地
cd "$PROJECT_DIR/output"
hdfs dfs -get /user/mushuting/output/flink_* . 2>/dev/null || true
# 重命名为标准文件名（去掉 flink_ 前缀）
for f in flink_*.txt; do
    if [ -f "$f" ]; then
        cp "$f" "${f#flink_}"
        echo "  ${f#flink_}"
    fi
done

# 生成图表
source ~/spark/venv/bin/activate
cd "$PROJECT_DIR/visualization"
python visualize_java.py

echo ""
echo "================================================"
echo "  全部完成！"
echo ""
echo "  Flink Web UI  : http://node3:8081"
echo "  HDFS Web UI   : http://node3:9870"
echo "  图表目录      : $PROJECT_DIR/visualization/"
echo "  输出数据      : $PROJECT_DIR/output/"
echo "================================================"
echo ""
echo "生成的图表:"
ls -la "$PROJECT_DIR/visualization/java_*.png" 2>/dev/null
echo ""
echo "停止服务:"
echo "  cd $FLINK_HOME && ./bin/stop-cluster.sh"
echo "  stop-dfs.sh"
