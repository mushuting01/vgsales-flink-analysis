#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flink数据分析可视化脚本 - 直接读取Flink输出文件（无表头）
生成9个图表对应9个分析任务
"""
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import os
from typing import Optional, List

plt.rcParams['figure.dpi'] = 150
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'SimHei', 'WenQuanYi Micro Hei']

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
CHART_DIR = os.path.join(BASE_DIR, "visualization")
os.makedirs(CHART_DIR, exist_ok=True)

# Flink各任务输出文件的列名定义（Flink输出无表头，需要手动指定）
COLUMN_MAP = {
    "01_console_stats.txt":      ["平台", "游戏数量", "总销售额", "平均销售额"],
    "02_genre_stats.txt":        ["游戏类型", "游戏数量", "总销售额", "平均销售额"],
    "03_publisher_top15.txt":    ["出版商", "游戏数量", "总销售额", "平均销售额"],
    "04_developer_top15.txt":    ["开发商", "游戏数量", "总销售额", "平均销售额"],
    "05_score_sales_corr.txt":   ["评分区间", "游戏数量", "总销售额", "平均销售额"],
    "06_yearly_trend.txt":       ["年份", "游戏数量", "总销售额", "平均销售额"],
    "07_yearly_top_genre.txt":   ["年份", "游戏类型", "数量"],
    "08_region_sales.txt":       ["地区", "总销售额"],
    "09_yearly_best_game.txt":   ["年份", "年度最佳游戏", "游戏类型", "总销售额"],
}


def read_output(filename: str, aggregate: bool = True) -> Optional[pd.DataFrame]:
    """读取Flink输出文件（无表头），根据预定义列名赋值，自动聚合去重"""
    path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(path):
        print(f"警告: 文件不存在 {path}")
        return None
    try:
        columns = COLUMN_MAP.get(filename)
        if columns is None:
            print(f"警告: 未知文件 {filename}，无列名定义")
            return None

        df = pd.read_csv(path, sep="|", header=None, engine='python',
                         encoding='utf-8', names=columns)
        # 去除字符串列的前后空格
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.strip()
        # 数值列转换
        for col in ["游戏数量", "总销售额", "平均销售额", "数量"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        raw_rows = len(df)

        # 自动聚合：按第一列（分组键）去重，取每组最后一行（Flink窗口最终结果）
        if aggregate and len(df) > 100:
            group_col = columns[0]
            # 对于数值列取max（窗口排序后最后一行的值），字符串列取last
            agg_dict = {}
            for col in columns[1:]:
                if col in ["游戏数量", "总销售额", "平均销售额", "数量"]:
                    agg_dict[col] = 'max'
                else:
                    agg_dict[col] = 'last'
            df = df.groupby(group_col, as_index=False).agg(agg_dict)
            print(f"  读取 {filename}: {raw_rows} -> {len(df)} 行 (已聚合), 列: {list(df.columns)}")
        else:
            print(f"  读取 {filename}: {len(df)} 行, 列: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"读取 {filename} 失败: {e}")
        import traceback
        traceback.print_exc()
        return None


print("=" * 50)
print("开始生成Flink数据分析可视化图表...")
print("=" * 50)

fig_num = 0

# 图1: 平台统计
df1 = read_output("01_console_stats.txt")
if df1 is not None:
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    df1_sorted = df1.sort_values("总销售额", ascending=True).tail(15)
    colors1 = plt.cm.Blues(np.linspace(0.3, 0.9, len(df1_sorted)))
    axes[0].barh(df1_sorted["平台"], df1_sorted["游戏数量"], color=colors1)
    axes[0].set_xlabel("Game Count")
    axes[0].set_title("Game Count by Platform (Top 15)")

    df1_sorted2 = df1.sort_values("总销售额", ascending=True).tail(15)
    colors2 = plt.cm.Greens(np.linspace(0.3, 0.9, len(df1_sorted2)))
    axes[1].barh(df1_sorted2["平台"], df1_sorted2["总销售额"], color=colors2)
    axes[1].set_xlabel("Total Sales (M)")
    axes[1].set_title("Total Sales by Platform (Top 15)")
    plt.tight_layout()
    plt.savefig(os.path.join(CHART_DIR, "java_01_console_stats.png"), bbox_inches='tight')
    plt.close()
    print("图1: 平台统计 - 完成")
    fig_num += 1

# 图2: 类型统计
df2 = read_output("02_genre_stats.txt")
if df2 is not None:
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    df2_pct = df2.copy()
    total = df2_pct["总销售额"].sum()
    df2_pct["占比"] = df2_pct["总销售额"] / total * 100 if total > 0 else 0
    mask = df2_pct["占比"] >= 2.0
    main = df2_pct[mask]
    pie_data = list(main["总销售额"]) + [df2_pct[~mask]["总销售额"].sum()]
    pie_labels = list(main["游戏类型"]) + ["Other"]
    colors = plt.cm.Set3(np.linspace(0, 1, len(pie_data)))
    axes[0].pie(pie_data, labels=pie_labels, autopct='%1.1f%%', colors=colors, startangle=90)
    axes[0].set_title("Sales Share by Genre")

    df2_sorted = df2.sort_values("平均销售额", ascending=True).tail(12)
    colors = plt.cm.Reds(np.linspace(0.3, 0.9, len(df2_sorted)))
    axes[1].barh(df2_sorted["游戏类型"], df2_sorted["平均销售额"], color=colors)
    axes[1].set_xlabel("Average Sales per Game (M)")
    axes[1].set_title("Average Sales by Genre")
    plt.tight_layout()
    plt.savefig(os.path.join(CHART_DIR, "java_02_genre_stats.png"), bbox_inches='tight')
    plt.close()
    print("图2: 类型统计 - 完成")
    fig_num += 1

# 图3: 出版商Top15
df3 = read_output("03_publisher_top15.txt")
if df3 is not None:
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    df3_sorted = df3.sort_values("总销售额", ascending=True)
    colors = plt.cm.Purples(np.linspace(0.3, 0.9, len(df3_sorted)))
    axes[0].barh(df3_sorted["出版商"], df3_sorted["总销售额"], color=colors)
    axes[0].set_xlabel("Total Sales (M)")
    axes[0].set_title("Top 15 Publishers by Total Sales")

    df3_sorted2 = df3.sort_values("平均销售额", ascending=True)
    colors = plt.cm.Oranges(np.linspace(0.3, 0.9, len(df3_sorted2)))
    axes[1].barh(df3_sorted2["出版商"], df3_sorted2["平均销售额"], color=colors)
    axes[1].set_xlabel("Average Sales per Game (M)")
    axes[1].set_title("Top 15 Publishers by Average Sales")
    plt.tight_layout()
    plt.savefig(os.path.join(CHART_DIR, "java_03_publisher_top15.png"), bbox_inches='tight')
    plt.close()
    print("图3: 出版商Top15 - 完成")
    fig_num += 1

# 图4: 开发商Top15
df4 = read_output("04_developer_top15.txt")
if df4 is not None:
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    df4_sorted = df4.sort_values("总销售额", ascending=True)
    colors = plt.cm.Purples(np.linspace(0.3, 0.9, len(df4_sorted)))
    axes[0].barh(df4_sorted["开发商"], df4_sorted["总销售额"], color=colors)
    axes[0].set_xlabel("Total Sales (M)")
    axes[0].set_title("Top 15 Developers by Total Sales")

    df4_sorted2 = df4.sort_values("平均销售额", ascending=True)
    colors = plt.cm.Oranges(np.linspace(0.3, 0.9, len(df4_sorted2)))
    axes[1].barh(df4_sorted2["开发商"], df4_sorted2["平均销售额"], color=colors)
    axes[1].set_xlabel("Average Sales per Game (M)")
    axes[1].set_title("Top 15 Developers by Average Sales")
    plt.tight_layout()
    plt.savefig(os.path.join(CHART_DIR, "java_04_developer_top15.png"), bbox_inches='tight')
    plt.close()
    print("图4: 开发商Top15 - 完成")
    fig_num += 1

# 图5: 评分与销量关系
df5 = read_output("05_score_sales_corr.txt")
if df5 is not None:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    colors = plt.cm.YlOrRd(np.linspace(0.3, 0.9, len(df5)))
    axes[0].bar(df5["评分区间"], df5["游戏数量"], color=colors)
    axes[0].set_xlabel("Score Range")
    axes[0].set_ylabel("Game Count")
    axes[0].set_title("Game Count by Score Range")
    axes[0].tick_params(axis='x', rotation=45)

    axes[1].plot(df5["评分区间"], df5["平均销售额"], 'o-', color='darkgreen', linewidth=2, markersize=8)
    axes[1].set_xlabel("Score Range")
    axes[1].set_ylabel("Average Sales (M)")
    axes[1].set_title("Score vs Average Sales Correlation")
    axes[1].tick_params(axis='x', rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(CHART_DIR, "java_05_score_sales_corr.png"), bbox_inches='tight')
    plt.close()
    print("图5: 评分与销量关系 - 完成")
    fig_num += 1

# 图6: 年度趋势
df6 = read_output("06_yearly_trend.txt")
if df6 is not None:
    fig, axes = plt.subplots(1, 2, figsize=(16, 5))
    df6["年份"] = df6["年份"].astype(str)
    df6 = df6[df6["年份"].str.match(r'\d{4}')]

    axes[0].plot(df6["年份"], df6["游戏数量"], 'o-', color='steelblue', linewidth=2)
    axes[0].fill_between(df6["年份"], df6["游戏数量"], alpha=0.3, color='steelblue')
    axes[0].set_xlabel("Year")
    axes[0].set_ylabel("Game Count")
    axes[0].set_title("Yearly Game Release Trend")
    axes[0].tick_params(axis='x', rotation=45)
    if len(df6) > 20:
        tick_locs = range(0, len(df6), len(df6) // 10)
        axes[0].set_xticks([df6.iloc[i]["年份"] for i in tick_locs])

    axes[1].plot(df6["年份"], df6["总销售额"], 's-', color='darkgreen', linewidth=2)
    axes[1].fill_between(df6["年份"], df6["总销售额"], alpha=0.3, color='darkgreen')
    axes[1].set_xlabel("Year")
    axes[1].set_ylabel("Total Sales (M)")
    axes[1].set_title("Yearly Total Sales Trend")
    axes[1].tick_params(axis='x', rotation=45)
    if len(df6) > 20:
        tick_locs = range(0, len(df6), len(df6) // 10)
        axes[1].set_xticks([df6.iloc[i]["年份"] for i in tick_locs])
    plt.tight_layout()
    plt.savefig(os.path.join(CHART_DIR, "java_06_yearly_trend.png"), bbox_inches='tight')
    plt.close()
    print("图6: 年度趋势 - 完成")
    fig_num += 1

# 图7: 年度主流类型
df7 = read_output("07_yearly_top_genre.txt")
if df7 is not None:
    df7["年份"] = df7["年份"].astype(str)
    df7 = df7[df7["年份"].str.match(r'\d{4}')]
    recent_years = df7.tail(20)

    fig, ax = plt.subplots(figsize=(16, 6))
    colors = plt.cm.Set3(np.linspace(0, 1, len(recent_years)))
    bars = ax.bar(recent_years["年份"], recent_years["数量"], color=colors)
    ax.set_xlabel("Year")
    ax.set_ylabel("Count of Top Genre")
    ax.set_title("Top Genre Count by Year (Recent 20 Years)")
    ax.tick_params(axis='x', rotation=45)
    # 在柱子上标注类型名称
    for bar, genre in zip(bars, recent_years["游戏类型"]):
        ax.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.5,
                str(genre)[:8], ha='center', va='bottom', fontsize=6, rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(CHART_DIR, "java_07_yearly_top_genre.png"), bbox_inches='tight')
    plt.close()
    print("图7: 年度主流类型 - 完成")
    fig_num += 1

# 图8: 地区销售额
df8 = read_output("08_region_sales.txt")
if df8 is not None:
    df8 = df8[~df8["地区"].str.contains("全球")]
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    colors = plt.cm.Set2(np.linspace(0, 1, len(df8)))

    axes[0].pie(df8["总销售额"], labels=df8["地区"], autopct='%1.1f%%',
                colors=colors, startangle=90)
    axes[0].set_title("Sales Share by Region")

    bars = axes[1].bar(df8["地区"], df8["总销售额"], color=colors)
    axes[1].set_ylabel("Total Sales (M)")
    axes[1].set_title("Total Sales by Region")
    for bar in bars:
        h = bar.get_height()
        axes[1].text(bar.get_x() + bar.get_width()/2., h,
                     f'{h:.1f}', ha='center', va='bottom')
    plt.tight_layout()
    plt.savefig(os.path.join(CHART_DIR, "java_08_region_sales.png"), bbox_inches='tight')
    plt.close()
    print("图8: 地区销售额 - 完成")
    fig_num += 1

# 图9: 年度最佳游戏
df9 = read_output("09_yearly_best_game.txt")
if df9 is not None:
    df9["年份"] = df9["年份"].astype(str)
    df9 = df9[df9["年份"].str.match(r'\d{4}')].sort_values(by="年份")

    fig, ax = plt.subplots(figsize=(14, 8))
    y_pos = range(len(df9))
    colors = plt.cm.viridis(np.linspace(0, 1, len(df9)))
    ax.barh(y_pos, df9["总销售额"], color=colors)
    ax.set_yticks(y_pos)
    labels = [f"{row['年份']} - {str(row['年度最佳游戏'])[:25]}" for _, row in df9.iterrows()]
    ax.set_yticklabels(labels, fontsize=7)
    ax.invert_yaxis()
    ax.set_xlabel("Total Sales (M)")
    ax.set_title("Best Selling Game of Each Year")
    plt.tight_layout()
    plt.savefig(os.path.join(CHART_DIR, "java_09_yearly_best_game.png"), bbox_inches='tight')
    plt.close()
    print("图9: 年度最佳游戏 - 完成")
    fig_num += 1

print("\n" + "=" * 50)
print(f"Flink数据分析可视化完成! 共生成 {fig_num} 张图表")
print(f"图表保存目录: {CHART_DIR}")
print("=" * 50)
