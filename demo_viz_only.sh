#!/bin/bash
# ================================================
#  仅生成可视化图表 (使用已有output数据)
#  用法: bash demo_viz_only.sh
# ================================================

echo "================================================"
echo "  生成可视化图表 (使用已有数据)"
echo "================================================"

PROJECT_DIR="/home/mushuting/vgsales-flink"

source ~/spark/venv/bin/activate
cd "$PROJECT_DIR/visualization"
python visualize_java.py

echo ""
echo "================================================"
echo "  图表生成完成!"
echo "  共 9 张图表:"
echo "================================================"
ls -la java_*.png 2>/dev/null
