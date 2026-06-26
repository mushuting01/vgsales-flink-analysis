package vgsales;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 *  Video Game Sales Flink 分析 — 本地模式
 *  输入：本地 CSV 文件
 *  输出：本地 output/ 目录下的 9 个 txt 文件
 */
public class VideoGameSalesFlinkAnalysisLocal {

    public static void main(String[] args) throws Exception {

        // ---------- 1. 参数解析 ----------
        String inputPath = "data/vgchartz-2024.csv";   // 默认输入路径
        String outputPrefix = "output/flink_";           // 默认输出前缀

        if (args.length >= 1) inputPath = args[0];     // 命令行第1个参数：输入文件
        if (args.length >= 2) outputPrefix = args[1];   // 命令行第2个参数：输出前缀

        System.out.println("============================================");
        System.out.println("  Flink本地测试模式");
        System.out.println("  输入文件: " + inputPath);
        System.out.println("  输出前缀: " + outputPrefix);
        System.out.println("============================================\n");

        // ---------- 2. 创建执行环境 ----------
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 本地单线程，方便输出顺序一致

        // ---------- 3. 读取 CSV 并解析为 GameRecord 流 ----------
        DataStream<String> text = env.readTextFile(inputPath);

        DataStream<GameRecord> vgsales = text
                .filter(line -> !line.startsWith("Rank")) // 跳过表头
                .map(line -> parseLine(line))              // 每行解析成 GameRecord
                .filter(record -> record != null && record.totalSales > 0); // 过滤脏数据

        // ---------- 4. 依次执行 9 个分析任务 ----------
        task1_ConsoleStats(vgsales, outputPrefix + "01_console_stats.txt");
        task2_GenreStats(vgsales, outputPrefix + "02_genre_stats.txt");
        task3_PublisherTop15(vgsales, outputPrefix + "03_publisher_top15.txt");
        task4_DeveloperTop15(vgsales, outputPrefix + "04_developer_top15.txt");
        task5_ScoreSalesCorrelation(vgsales, outputPrefix + "05_score_sales_corr.txt");
        task6_YearlyTrend(vgsales, outputPrefix + "06_yearly_trend.txt");
        task7_YearlyTopGenre(vgsales, outputPrefix + "07_yearly_top_genre.txt");
        task8_RegionSales(vgsales, outputPrefix + "08_region_sales.txt");
        task9_YearlyBestGame(vgsales, outputPrefix + "09_yearly_best_game.txt");

        // ---------- 5. 触发执行（Flink 懒执行，必须显式调用） ----------
        env.execute("Video Game Sales Flink Analysis - Local Mode");
    }

    // ===================== Task 1：各平台销量统计 =====================
    private static void task1_ConsoleStats(DataStream<GameRecord> vgsales, String outputPath) {
        // map: (平台, 游戏数量1, 销售额) → keyBy(平台) → reduce 增量聚合
        DataStream<Tuple3<String, Long, Float>> platformSum = vgsales
                .map(value -> new Tuple3<>(value.console, 1L, (float) value.totalSales))
                .keyBy(0)
                .reduce((v1, v2) -> new Tuple3<>(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2));

        // 全局排序：keyBy(1) 全部分到同一个分区，窗口内排序
        DataStream<Tuple3<String, Long, Float>> sorted = platformSum
                .keyBy(value -> 1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new SortProcessFunction());

        // 格式化输出并写入文件
        sorted.map(value -> String.format("%s|%d|%.2f|%.2f", value.f0, value.f1, value.f2,
                        value.f1 > 0 ? value.f2 / value.f1 : 0))
                .writeAsText(outputPath).setParallelism(1);

        System.out.println("任务1配置完成: 平台统计 - 按销售额排序");
    }

    // ===================== Task 2：各类型销量统计 =====================
    private static void task2_GenreStats(DataStream<GameRecord> vgsales, String outputPath) {
        DataStream<Tuple3<String, Long, Float>> genreSum = vgsales
                .map(value -> new Tuple3<>(value.genre, 1L, (float) value.totalSales))
                .keyBy(0)
                .reduce((v1, v2) -> new Tuple3<>(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2));

        DataStream<Tuple3<String, Long, Float>> sorted = genreSum
                .keyBy(value -> 1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new SortProcessFunction());

        sorted.map(value -> String.format("%s|%d|%.2f|%.2f", value.f0, value.f1, value.f2,
                        value.f1 > 0 ? value.f2 / value.f1 : 0))
                .writeAsText(outputPath).setParallelism(1);

        System.out.println("任务2配置完成: 类型统计");
    }

    // ===================== Task 3：出版商 Top15 =====================
    private static void task3_PublisherTop15(DataStream<GameRecord> vgsales, String outputPath) {
        DataStream<Tuple3<String, Long, Float>> publisherSum = vgsales
                .map(value -> new Tuple3<>(value.publisher, 1L, (float) value.totalSales))
                .keyBy(0)
                .reduce((v1, v2) -> new Tuple3<>(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2));

        // TopNProcessFunction(15) 取前 15 名
        DataStream<Tuple3<String, Long, Float>> top = publisherSum
                .keyBy(value -> 1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new TopNProcessFunction(15));

        top.map(value -> String.format("%s|%d|%.2f|%.2f", value.f0, value.f1, value.f2,
                        value.f1 > 0 ? value.f2 / value.f1 : 0))
                .writeAsText(outputPath).setParallelism(1);

        System.out.println("任务3配置完成: 出版商Top15");
    }

    // ===================== Task 4：开发商 Top15 =====================
    private static void task4_DeveloperTop15(DataStream<GameRecord> vgsales, String outputPath) {
        DataStream<Tuple3<String, Long, Float>> developerSum = vgsales
                .map(value -> new Tuple3<>(value.developer, 1L, (float) value.totalSales))
                .keyBy(0)
                .reduce((v1, v2) -> new Tuple3<>(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2));

        DataStream<Tuple3<String, Long, Float>> top = developerSum
                .keyBy(value -> 1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new TopNProcessFunction(15));

        top.map(value -> String.format("%s|%d|%.2f|%.2f", value.f0, value.f1, value.f2,
                        value.f1 > 0 ? value.f2 / value.f1 : 0))
                .writeAsText(outputPath).setParallelism(1);

        System.out.println("任务4配置完成: 开发商Top15");
    }

    // ===================== Task 5：评分与销量关系 =====================
    private static void task5_ScoreSalesCorrelation(DataStream<GameRecord> vgsales, String outputPath) {
        // 按评分区间分桶（9.0-10.0、8.0-8.9 ...），统计每个桶的总销量和游戏数
        DataStream<Tuple3<String, Long, Float>> scoreBucket = vgsales
                .filter(record -> record.criticScore > 0)
                .map(value -> new Tuple3<>(getScoreBucket(value.criticScore), 1L, (float) value.totalSales))
                .keyBy(0)
                .reduce((v1, v2) -> new Tuple3<>(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2));

        scoreBucket.map(value -> String.format("%s|%d|%.2f|%.2f", value.f0, value.f1, value.f2,
                        value.f1 > 0 ? value.f2 / value.f1 : 0))
                .writeAsText(outputPath).setParallelism(1);

        System.out.println("任务5配置完成: 评分与销量关系分析");
    }

    // ===================== Task 6：年度销售趋势 =====================
    private static void task6_YearlyTrend(DataStream<GameRecord> vgsales, String outputPath) {
        DataStream<Tuple3<String, Long, Float>> yearlySum = vgsales
                .filter(record -> !record.releaseYear.isEmpty())
                .map(value -> new Tuple3<>(value.releaseYear, 1L, (float) value.totalSales))
                .keyBy(0)
                .reduce((v1, v2) -> new Tuple3<>(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2));

        yearlySum.map(value -> String.format("%s|%d|%.2f|%.2f", value.f0, value.f1, value.f2,
                        value.f1 > 0 ? value.f2 / value.f1 : 0))
                .writeAsText(outputPath).setParallelism(1);

        System.out.println("任务6配置完成: 年度趋势");
    }

    // ===================== Task 7：每年最主流的游戏类型 =====================
    private static void task7_YearlyTopGenre(DataStream<GameRecord> vgsales, String outputPath) {
        // (年份, 类型, 游戏数量) → keyBy(年份, 类型) → reduce 聚合
        DataStream<Tuple3<String, String, Long>> yearGenre = vgsales
                .filter(record -> !record.releaseYear.isEmpty() && !record.genre.isEmpty())
                .map(value -> new Tuple3<>(value.releaseYear, value.genre, 1L))
                .keyBy(0, 1)
                .reduce((v1, v2) -> new Tuple3<>(v1.f0, v1.f1, v1.f2 + v2.f2));

        // 每年取数量最多的 1 个类型（即当年最主流类型）
        DataStream<Tuple3<String, String, Long>> topGenre = yearGenre
                .keyBy(value -> value.f0)       // 按年份分组
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new TopNProcessFunction2(1));

        topGenre.map(value -> String.format("%s|%s|%d", value.f0, value.f1, value.f2))
                .writeAsText(outputPath).setParallelism(1);

        System.out.println("任务7配置完成: 年度主流类型");
    }

    // ===================== Task 8：各地区销售额 =====================
    private static void task8_RegionSales(DataStream<GameRecord> vgsales, String outputPath) {
        // flatMap：一条记录拆成 4 条（NA / JP / PAL / Other）
        DataStream<Tuple2<String, Float>> regionSum = vgsales
                .flatMap((value, out) -> {
                    out.collect(new Tuple2<>("NA", (float) value.naSales));
                    out.collect(new Tuple2<>("JP", (float) value.jpSales));
                    out.collect(new Tuple2<>("PAL", (float) value.palSales));
                    out.collect(new Tuple2<>("Other", (float) value.otherSales));
                })
                .keyBy(0)
                .reduce((v1, v2) -> new Tuple2<>(v1.f0, v1.f1 + v2.f1));

        regionSum.map(value -> String.format("%s|%.2f", value.f0, value.f1))
                .writeAsText(outputPath).setParallelism(1);

        System.out.println("任务8配置完成: 地区销售额");
    }

    // ===================== Task 9：每年销售额最高的游戏 =====================
    private static void task9_YearlyBestGame(DataStream<GameRecord> vgsales, String outputPath) {
        // (年份, 游戏名, 类型, 销售额)
        DataStream<Tuple4<String, String, String, Float>> yearGame = vgsales
                .filter(record -> !record.releaseYear.isEmpty())
                .map(value -> new Tuple4<>(value.releaseYear, value.title, value.genre, (float) value.totalSales));

        // maxBy(3)：按第4个字段（销售额）取最大值所在的完整记录
        DataStream<Tuple4<String, String, String, Float>> best = yearGame
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .maxBy(3);

        best.map(value -> String.format("%s|%s|%s|%.2f", value.f0, value.f1, value.f2, value.f3))
                .writeAsText(outputPath).setParallelism(1);

        System.out.println("任务9配置完成: 年度最佳游戏");
    }

    // ===================== 工具方法 =====================

    // 将评分映射为区间字符串
    private static String getScoreBucket(double score) {
        if (score >= 9.0) return "9.0-10.0";
        if (score >= 8.0) return "8.0-8.9";
        if (score >= 7.0) return "7.0-7.9";
        if (score >= 6.0) return "6.0-6.9";
        if (score >= 5.0) return "5.0-5.9";
        return "0-4.9";
    }

    // 手写 CSV 解析器（处理字段内带引号+逗号的情况）
    public static GameRecord parseLine(String line) {
        try {
            List<String> fields = new ArrayList<>();
            boolean inQuotes = false;           // 是否在引号内部
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                if (c == '"') {
                    inQuotes = !inQuotes;       // 切换引号状态
                } else if (c == ',' && !inQuotes) {
                    fields.add(sb.toString());   // 逗号分隔字段（引号内逗号不算分隔符）
                    sb = new StringBuilder();
                } else {
                    sb.append(c);
                }
            }
            fields.add(sb.toString());           // 最后一个字段

            if (fields.size() < 12) return null;

            GameRecord g = new GameRecord();
            g.title        = fields.get(1).trim();
            g.console      = fields.get(2).trim();
            g.genre        = fields.get(3).trim();
            g.publisher    = fields.get(4).trim();
            g.developer    = fields.size() > 5 ? fields.get(5).trim() : "";
            try { g.criticScore = Double.parseDouble(fields.get(6).trim()); } catch (Exception e) {}
            try { g.totalSales  = Double.parseDouble(fields.get(7).trim()); } catch (Exception e) {}
            try { g.naSales     = Double.parseDouble(fields.get(8).trim()); } catch (Exception e) {}
            try { g.jpSales     = Double.parseDouble(fields.get(9).trim()); } catch (Exception e) {}
            try { g.palSales    = Double.parseDouble(fields.get(10).trim()); } catch (Exception e) {}
            try { g.otherSales  = Double.parseDouble(fields.get(11).trim()); } catch (Exception e) {}
            if (fields.size() > 12) {
                String rd = fields.get(12).trim();
                g.releaseYear = rd.length() >= 4 ? rd.substring(0, 4) : "";
            }
            return g;
        } catch (Exception e) {
            return null; // 解析失败返回 null，后续 filter 会过滤掉
        }
    }

    // ===================== 数据模型 =====================
    static class GameRecord {
        String title = "";
        String console = "";
        String genre = "";
        String publisher = "";
        String developer = "";
        double criticScore = 0;
        double totalSales = 0;
        double naSales = 0;
        double jpSales = 0;
        double palSales = 0;
        double otherSales = 0;
        String releaseYear = "";
    }

    // ===================== 窗口函数 =====================

    // 对窗口内所有元素按 f2（销售额）降序排序
    static class SortProcessFunction extends
            ProcessWindowFunction<Tuple3<String, Long, Float>, Tuple3<String, Long, Float>, Integer, TimeWindow> {
        @Override
        public void process(Integer key, Context ctx,
                            Iterable<Tuple3<String, Long, Float>> elements,
                            Collector<Tuple3<String, Long, Float>> out) {
            List<Tuple3<String, Long, Float>> list = new ArrayList<>();
            for (Tuple3<String, Long, Float> e : elements) list.add(e);
            list.sort((a, b) -> Float.compare(b.f2, a.f2)); // 降序
            for (Tuple3<String, Long, Float> e : list) out.collect(e);
        }
    }

    // 取窗口内销售额前 N 名
    static class TopNProcessFunction extends
            ProcessWindowFunction<Tuple3<String, Long, Float>, Tuple3<String, Long, Float>, Integer, TimeWindow> {
        private final int n;
        TopNProcessFunction(int n) { this.n = n; }

        @Override
        public void process(Integer key, Context ctx,
                            Iterable<Tuple3<String, Long, Float>> elements,
                            Collector<Tuple3<String, Long, Float>> out) {
            List<Tuple3<String, Long, Float>> list = new ArrayList<>();
            for (Tuple3<String, Long, Float> e : elements) list.add(e);
            list.sort((a, b) -> Float.compare(b.f2, a.f2));
            for (int i = 0; i < Math.min(n, list.size()); i++) out.collect(list.get(i));
        }
    }

    // 取窗口内数量前 N 名（按 Long 比较，用于游戏数量排序）
    static class TopNProcessFunction2 extends
            ProcessWindowFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String, TimeWindow> {
        private final int n;
        TopNProcessFunction2(int n) { this.n = n; }

        @Override
        public void process(String key, Context ctx,
                            Iterable<Tuple3<String, String, Long>> elements,
                            Collector<Tuple3<String, String, Long>> out) {
            List<Tuple3<String, String, Long>> list = new ArrayList<>();
            for (Tuple3<String, String, Long> e : elements) list.add(e);
            list.sort((a, b) -> Long.compare(b.f2, a.f2));
            for (int i = 0; i < Math.min(n, list.size()); i++) out.collect(list.get(i));
        }
    }
}