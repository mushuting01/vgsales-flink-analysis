#!/bin/bash
# ================================================
#  一键演示脚本 - 运行Flink分析 + 生成可视化图表
#  用法: bash demo_all.sh
# ================================================

echo "================================================"
echo "  游戏销售数据分析 - Flink演示脚本"
echo "================================================"
echo ""

PROJECT_DIR="/home/mushuting/vgsales-flink"

# 激活Python虚拟环境
source ~/spark/venv/bin/activate

cd "$PROJECT_DIR"

# ========== 第1步: 清空旧输出 ==========
echo "[1/3] 清空旧输出文件..."
rm -f output/flink_*.txt output/0*.txt
echo "  完成"

# ========== 第2步: 运行Flink分析 ==========
echo ""
echo "[2/3] 运行Flink数据分析 (需要1-2分钟)..."
echo "  正在启动Flink本地环境..."

MAVEN_OPTS="-Xmx1024m --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED" mvn exec:java -Dexec.mainClass="vgsales.VideoGameSalesFlinkAnalysisLocal" -q 2>&1

# 处理输出文件名 (去掉 flink_ 前缀)
echo ""
echo "  整理输出文件..."
cd "$PROJECT_DIR/output"
for f in flink_*.txt; do
    if [ -f "$f" ]; then
        cp "$f" "${f#flink_}"
        echo "    ${f#flink_}"
    fi
done
# 修复Flink可能产生的文件名编号不一致问题
if [ -f "10_yearly_best_game.txt" ] && [ ! -f "09_yearly_best_game.txt" ]; then
    cp "10_yearly_best_game.txt" "09_yearly_best_game.txt"
    echo "    09_yearly_best_game.txt (从10_修复)"
fi
cd "$PROJECT_DIR"
echo "  Flink分析完成"

# ========== 第3步: 生成图表 ==========
echo ""
echo "[3/3] 生成可视化图表..."
cd "$PROJECT_DIR/visualization"
python visualize_java.py
cd "$PROJECT_DIR"

# ========== 完成 ==========
echo ""
echo "================================================"
echo "  全部完成!"
echo "  图表保存在: $PROJECT_DIR/visualization/"
echo "  输出数据在: $PROJECT_DIR/output/"
echo "================================================"
echo ""
echo "生成的文件:"
ls -la "$PROJECT_DIR/visualization/java_*.png" 2>/dev/null
echo ""
echo "查看图表: cd $PROJECT_DIR/visualization && ls java_*.png"
