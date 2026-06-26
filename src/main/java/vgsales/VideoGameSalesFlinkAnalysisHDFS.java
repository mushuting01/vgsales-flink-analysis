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
 * Flink HDFS 集群部署版 —— 视频游戏销售数据分析
 * 用法: flink run -c vgsales.VideoGameSalesFlinkAnalysisHDFS vgsales-flink-1.0-all.jar \
 *         hdfs://node3:9000/user/mushuting/input/vgchartz-2024.csv \
 *         hdfs://node3:9000/user/mushuting/output/flink_
 */
public class VideoGameSalesFlinkAnalysisHDFS {

    public static void main(String[] args) throws Exception {
        // 默认 HDFS 路径（可通过命令行参数覆盖）
        String inputPath = "hdfs://node3:9000/user/mushuting/input/vgchartz-2024.csv";
        String outputPrefix = "hdfs://node3:9000/user/mushuting/output/flink_";

        if (args.length >= 1) inputPath = args[0];
        if (args.length >= 2) outputPrefix = args[1];

        System.out.println("============================================");
        System.out.println("  Flink HDFS集群模式 (Web UI: http://node3:8081)");
        System.out.println("  输入: " + inputPath);
        System.out.println("  输出: " + outputPrefix);
        System.out.println("============================================\n");

        // 创建流式执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从 HDFS 读取 CSV 文件
        DataStream<String> text = env.readTextFile(inputPath);

        // 数据清洗：去表头 -> 解析 -> 过滤无效记录
        DataStream<GameRecord> vgsales = text
                .filter(line -> !line.startsWith("Rank"))      // 去掉 CSV 表头行
                .map(new MapFunction<String, GameRecord>() {   // 每行字符串->GameRecord 对象
                    @Override
                    public GameRecord map(String line) throws Exception {
                        return parseLine(line);
                    }
                })
                .filter(record -> record != null && record.totalSales > 0); // 过滤解析失败和销售额为 0 的记录

        // 注册 9 个分析任务（此时只是构建 DAG，尚未执行）
        //描述每一条数据该经过哪些算子、怎么流动。
        task1_ConsoleStats(vgsales, outputPrefix + "01_console_stats.txt");
        task2_GenreStats(vgsales, outputPrefix + "02_genre_stats.txt");
        task3_PublisherTop15(vgsales, outputPrefix + "03_publisher_top15.txt");
        task4_DeveloperTop15(vgsales, outputPrefix + "04_developer_top15.txt");
        task5_ScoreSalesCorrelation(vgsales, outputPrefix + "05_score_sales_corr.txt");
        task6_YearlyTrend(vgsales, outputPrefix + "06_yearly_trend.txt");
        task7_YearlyTopGenre(vgsales, outputPrefix + "07_yearly_top_genre.txt");
        task8_RegionSales(vgsales, outputPrefix + "08_region_sales.txt");
        task9_YearlyBestGame(vgsales, outputPrefix + "09_yearly_best_game.txt");

        // 触发所有任务执行
        //JobManager 才把 DAG 解析成可执行的任务，分发给 TaskManager 真正开始计算
        env.execute("VGSales Flink Analysis - HDFS Mode");
    }

    //  ====================

    // 任务1: 各平台游戏数量+总销售额（按销售额降序）
    private static void task1_ConsoleStats(DataStream<GameRecord> vgsales, String outputPath) {
        DataStream<Tuple3<String, Long, Float>> platformSum = vgsales
                .map(new MapFunction<GameRecord, Tuple3<String, Long, Float>>() {
                    @Override
                    public Tuple3<String, Long, Float> map(GameRecord value) {
                        return new Tuple3<>(value.console, 1L, (float) value.totalSales);
                    }
                })
                .keyBy(0)    // 按平台名分组
                .reduce(new ReduceFunction<Tuple3<String, Long, Float>>() {
                    @Override
                    public Tuple3<String, Long, Float> reduce(Tuple3<String, Long, Float> v1, Tuple3<String, Long, Float> v2) {
                        return new Tuple3<>(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2); // 累加数量和销售额
                    }
                });
        // 窗口内排序
        DataStream<Tuple3<String, Long, Float>> sorted = platformSum
                .keyBy(value -> 1)    // 全部分到同一分区
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new SortProcessFunction()); // 按销售额降序排序
        // 格式化输出: 平台|数量|总销售额|平均销售额
        sorted.map(new MapFunction<Tuple3<String, Long, Float>, String>() {
            @Override
            public String map(Tuple3<String, Long, Float> value) {
                return String.format("%s|%d|%.2f|%.2f", value.f0, value.f1, value.f2, value.f1 > 0 ? value.f2 / value.f1 : 0);
            }
        }).writeAsText(outputPath).setParallelism(1);
        System.out.println("任务1配置完成: 平台统计");
    }

    // 任务2: 各类型游戏数量+总销售额（按销售额降序）
    private static void task2_GenreStats(DataStream<GameRecord> vgsales, String outputPath) {
        DataStream<Tuple3<String, Long, Float>> genreSum = vgsales
                .map(new MapFunction<GameRecord, Tuple3<String, Long, Float>>() {
                    @Override
                    public Tuple3<String, Long, Float> map(GameRecord value) {
                        return new Tuple3<>(value.genre, 1L, (float) value.totalSales);
                    }
                })
                .keyBy(0)    // 按类型名分组
                .reduce(new ReduceFunction<Tuple3<String, Long, Float>>() {
                    @Override
                    public Tuple3<String, Long, Float> reduce(Tuple3<String, Long, Float> v1, Tuple3<String, Long, Float> v2) {
                        return new Tuple3<>(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2);
                    }
                });
        DataStream<Tuple3<String, Long, Float>> sorted = genreSum
                .keyBy(value -> 1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new SortProcessFunction());
        sorted.map(new MapFunction<Tuple3<String, Long, Float>, String>() {
            @Override
            public String map(Tuple3<String, Long, Float> value) {
                return String.format("%s|%d|%.2f|%.2f", value.f0, value.f1, value.f2, value.f1 > 0 ? value.f2 / value.f1 : 0);
            }
        }).writeAsText(outputPath).setParallelism(1);
        System.out.println("任务2配置完成: 类型统计");
    }

    // 任务3: 出版商按总销售额 Top15
    private static void task3_PublisherTop15(DataStream<GameRecord> vgsales, String outputPath) {
        DataStream<Tuple3<String, Long, Float>> pubSum = vgsales
                .map(new MapFunction<GameRecord, Tuple3<String, Long, Float>>() {
                    @Override
                    public Tuple3<String, Long, Float> map(GameRecord value) {
                        return new Tuple3<>(value.publisher, 1L, (float) value.totalSales);
                    }
                })
                .keyBy(0)    // 按出版商名分组
                .reduce(new ReduceFunction<Tuple3<String, Long, Float>>() {
                    @Override
                    public Tuple3<String, Long, Float> reduce(Tuple3<String, Long, Float> v1, Tuple3<String, Long, Float> v2) {
                        return new Tuple3<>(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2);
                    }
                });
        DataStream<Tuple3<String, Long, Float>> top = pubSum
                .keyBy(value -> 1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new TopNProcessFunction(15)); // 只取前 15 名
        top.map(new MapFunction<Tuple3<String, Long, Float>, String>() {
            @Override
            public String map(Tuple3<String, Long, Float> value) {
                return String.format("%s|%d|%.2f|%.2f", value.f0, value.f1, value.f2, value.f1 > 0 ? value.f2 / value.f1 : 0);
            }
        }).writeAsText(outputPath).setParallelism(1);
        System.out.println("任务3配置完成: 出版商Top15");
    }

    // 任务4: 开发商按总销售额 Top15
    private static void task4_DeveloperTop15(DataStream<GameRecord> vgsales, String outputPath) {
        DataStream<Tuple3<String, Long, Float>> devSum = vgsales
                .map(new MapFunction<GameRecord, Tuple3<String, Long, Float>>() {
                    @Override
                    public Tuple3<String, Long, Float> map(GameRecord value) {
                        return new Tuple3<>(value.developer, 1L, (float) value.totalSales);
                    }
                })
                .keyBy(0)    // 按开发商名分组
                .reduce(new ReduceFunction<Tuple3<String, Long, Float>>() {
                    @Override
                    public Tuple3<String, Long, Float> reduce(Tuple3<String, Long, Float> v1, Tuple3<String, Long, Float> v2) {
                        return new Tuple3<>(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2);
                    }
                });
        DataStream<Tuple3<String, Long, Float>> top = devSum
                .keyBy(value -> 1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new TopNProcessFunction(15)); // 只取前 15 名
        top.map(new MapFunction<Tuple3<String, Long, Float>, String>() {
            @Override
            public String map(Tuple3<String, Long, Float> value) {
                return String.format("%s|%d|%.2f|%.2f", value.f0, value.f1, value.f2, value.f1 > 0 ? value.f2 / value.f1 : 0);
            }
        }).writeAsText(outputPath).setParallelism(1);
        System.out.println("任务4配置完成: 开发商Top15");
    }

    // 任务5: 评分区间 vs 销量关系
    private static void task5_ScoreSalesCorrelation(DataStream<GameRecord> vgsales, String outputPath) {
        DataStream<Tuple3<String, Long, Float>> scoreBucket = vgsales
                .filter(record -> record.criticScore > 0) // 排除无评分的游戏
                .map(new MapFunction<GameRecord, Tuple3<String, Long, Float>>() {
                    @Override
                    public Tuple3<String, Long, Float> map(GameRecord value) {
                        return new Tuple3<>(getScoreBucket(value.criticScore), 1L, (float) value.totalSales);
                    }
                })
                .keyBy(0)    // 按评分区间分组（如 "8.0-8.9"）
                .reduce(new ReduceFunction<Tuple3<String, Long, Float>>() {
                    @Override
                    public Tuple3<String, Long, Float> reduce(Tuple3<String, Long, Float> v1, Tuple3<String, Long, Float> v2) {
                        return new Tuple3<>(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2);
                    }
                });
        scoreBucket.map(new MapFunction<Tuple3<String, Long, Float>, String>() {
            @Override
            public String map(Tuple3<String, Long, Float> value) {
                return String.format("%s|%d|%.2f|%.2f", value.f0, value.f1, value.f2, value.f1 > 0 ? value.f2 / value.f1 : 0);
            }
        }).writeAsText(outputPath).setParallelism(1);
        System.out.println("任务5配置完成: 评分与销量关系");
    }

    // 任务6: 年度趋势（每年游戏数量+总销售额）
    private static void task6_YearlyTrend(DataStream<GameRecord> vgsales, String outputPath) {
        DataStream<Tuple3<String, Long, Float>> yearlySum = vgsales
                .filter(record -> !record.releaseYear.isEmpty()) // 排除无年份的记录
                .map(new MapFunction<GameRecord, Tuple3<String, Long, Float>>() {
                    @Override
                    public Tuple3<String, Long, Float> map(GameRecord value) {
                        return new Tuple3<>(value.releaseYear, 1L, (float) value.totalSales);
                    }
                })
                .keyBy(0)    // 按年份分组
                .reduce(new ReduceFunction<Tuple3<String, Long, Float>>() {
                    @Override
                    public Tuple3<String, Long, Float> reduce(Tuple3<String, Long, Float> v1, Tuple3<String, Long, Float> v2) {
                        return new Tuple3<>(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2);
                    }
                });
        yearlySum.map(new MapFunction<Tuple3<String, Long, Float>, String>() {
            @Override
            public String map(Tuple3<String, Long, Float> value) {
                return String.format("%s|%d|%.2f|%.2f", value.f0, value.f1, value.f2, value.f1 > 0 ? value.f2 / value.f1 : 0);
            }
        }).writeAsText(outputPath).setParallelism(1);
        System.out.println("任务6配置完成: 年度趋势");
    }

    // 任务7: 每年发布数量最多的游戏类型
    private static void task7_YearlyTopGenre(DataStream<GameRecord> vgsales, String outputPath) {
        DataStream<Tuple3<String, String, Long>> yearGenre = vgsales
                .filter(record -> !record.releaseYear.isEmpty() && !record.genre.isEmpty())
                .map(new MapFunction<GameRecord, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(GameRecord value) {
                        return new Tuple3<>(value.releaseYear, value.genre, 1L);
                    }
                })
                .keyBy(0, 1) // 按 (年份, 类型) 双字段联合分组
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> v1, Tuple3<String, String, Long> v2) {
                        return new Tuple3<>(v1.f0, v1.f1, v1.f2 + v2.f2);
                    }
                });
        DataStream<Tuple3<String, String, Long>> top = yearGenre
                .keyBy(value -> value.f0) // 按年份分组
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new TopNProcessFunction2(1)); // 每窗口取第1名
        top.map(new MapFunction<Tuple3<String, String, Long>, String>() {
            @Override
            public String map(Tuple3<String, String, Long> value) {
                return String.format("%s|%s|%d", value.f0, value.f1, value.f2);
            }
        }).writeAsText(outputPath).setParallelism(1);
        System.out.println("任务7配置完成: 年度主流类型");
    }

    // 任务8: 各地区(NA/JP/PAL/Other)销售额统计
    private static void task8_RegionSales(DataStream<GameRecord> vgsales, String outputPath) {
        DataStream<Tuple2<String, Float>> regionSum = vgsales
                .flatMap(new FlatMapFunction<GameRecord, Tuple2<String, Float>>() {
                    @Override
                    public void flatMap(GameRecord value, Collector<Tuple2<String, Float>> out) {
                        // 一条游戏记录拆成 4 条地区记录（一对多变换）
                        out.collect(new Tuple2<>("NA", (float) value.naSales));
                        out.collect(new Tuple2<>("JP", (float) value.jpSales));
                        out.collect(new Tuple2<>("PAL", (float) value.palSales));
                        out.collect(new Tuple2<>("Other", (float) value.otherSales));
                    }
                })
                .keyBy(0)    // 按地区名分组
                .reduce(new ReduceFunction<Tuple2<String, Float>>() {
                    @Override
                    public Tuple2<String, Float> reduce(Tuple2<String, Float> v1, Tuple2<String, Float> v2) {
                        return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
                    }
                });
        regionSum.map(new MapFunction<Tuple2<String, Float>, String>() {
            @Override
            public String map(Tuple2<String, Float> value) {
                return String.format("%s|%.2f", value.f0, value.f1);
            }
        }).writeAsText(outputPath).setParallelism(1);
        System.out.println("任务8配置完成: 地区销售额");
    }

    // 任务9: 每年最畅销的游戏（按销售额）
    private static void task9_YearlyBestGame(DataStream<GameRecord> vgsales, String outputPath) {
        DataStream<Tuple4<String, String, String, Float>> yearGame = vgsales
                .filter(record -> !record.releaseYear.isEmpty())
                .map(new MapFunction<GameRecord, Tuple4<String, String, String, Float>>() {
                    @Override
                    public Tuple4<String, String, String, Float> map(GameRecord value) {
                        return new Tuple4<>(value.releaseYear, value.title, value.genre, (float) value.totalSales);
                    }
                });
        DataStream<Tuple4<String, String, String, Float>> yearlyBest = yearGame
                .keyBy(0)                                          // 按年份分组
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .maxBy(3);                                         // 取销售额(f3)最大的完整记录
        yearlyBest.map(new MapFunction<Tuple4<String, String, String, Float>, String>() {
            @Override
            public String map(Tuple4<String, String, String, Float> value) {
                return String.format("%s|%s|%s|%.2f", value.f0, value.f1, value.f2, value.f3);
            }
        }).writeAsText(outputPath).setParallelism(1);
        System.out.println("任务9配置完成: 年度最佳游戏");
    }

    // 评分区间划分: 0-4.9 / 5.0-5.9 / ... / 9.0-10.0
    private static String getScoreBucket(double score) {
        if (score >= 9.0) return "9.0-10.0";
        if (score >= 8.0) return "8.0-8.9";
        if (score >= 7.0) return "7.0-7.9";
        if (score >= 6.0) return "6.0-6.9";
        if (score >= 5.0) return "5.0-5.9";
        return "0-4.9";
    }

    // CSV 行解析: 手写解析器，用 inQuotes 处理字段内含逗号的情况
    public static GameRecord parseLine(String line) {
        try {
            List<String> fields = new ArrayList<>();
            boolean inQuotes = false;         // 是否在双引号内部
            StringBuilder current = new StringBuilder();

            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                if (c == '"') {
                    inQuotes = !inQuotes;      // 遇到引号切换状态
                } else if (c == ',' && !inQuotes) {
                    fields.add(current.toString()); // 引号外的逗号=分隔符
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            }
            fields.add(current.toString());    // 最后一个字段

            if (fields.size() < 12) return null; // 字段不足，解析失败

            // 按 CSV 列序提取字段（索引0=Rank, 1=Name, 2=Console, ...）
            GameRecord g = new GameRecord();
            g.title     = fields.get(1).trim();
            g.console   = fields.get(2).trim();
            g.genre     = fields.get(3).trim();
            g.publisher = fields.get(4).trim();
            g.developer = fields.size() > 5 ? fields.get(5).trim() : "";
            try { g.criticScore  = Double.parseDouble(fields.get(6).trim()); }  catch (Exception e) {}
            try { g.totalSales   = Double.parseDouble(fields.get(7).trim()); }  catch (Exception e) {}
            try { g.naSales      = Double.parseDouble(fields.get(8).trim()); }  catch (Exception e) {}
            try { g.jpSales      = Double.parseDouble(fields.get(9).trim()); }  catch (Exception e) {}
            try { g.palSales     = Double.parseDouble(fields.get(10).trim()); } catch (Exception e) {}
            try { g.otherSales   = Double.parseDouble(fields.get(11).trim()); } catch (Exception e) {}
            if (fields.size() > 12) {
                String rd = fields.get(12).trim();
                g.releaseYear = rd.length() >= 4 ? rd.substring(0, 4) : ""; // 取前4位年份
            }
            return g;
        } catch (Exception e) { return null; }
    }

    // 游戏记录数据结构（对应 CSV 的一行）
    static class GameRecord {
        String title = "", console = "", genre = "", publisher = "", developer = "";
        double criticScore = 0, totalSales = 0, naSales = 0, jpSales = 0, palSales = 0, otherSales = 0;
        String releaseYear = "";
    }

    // 窗口内排序函数: 收集全部元素 -> 按销售额(f2)降序 -> 逐个输出
    static class SortProcessFunction extends
            ProcessWindowFunction<Tuple3<String, Long, Float>, Tuple3<String, Long, Float>, Integer, TimeWindow> {
        @Override
        public void process(Integer key, Context context,
                            Iterable<Tuple3<String, Long, Float>> elements, Collector<Tuple3<String, Long, Float>> out) {
            List<Tuple3<String, Long, Float>> list = new ArrayList<>();
            for (Tuple3<String, Long, Float> e : elements) list.add(e);
            list.sort((a, b) -> Float.compare(b.f2, a.f2)); // 按销售额降序
            for (Tuple3<String, Long, Float> e : list) out.collect(e);
        }
    }

    // TopN 函数(销售额版): 窗口内排序后只输出前 N 条
    static class TopNProcessFunction extends
            ProcessWindowFunction<Tuple3<String, Long, Float>, Tuple3<String, Long, Float>, Integer, TimeWindow> {
        private final int n;
        public TopNProcessFunction(int n) { this.n = n; }
        @Override
        public void process(Integer key, Context context,
                            Iterable<Tuple3<String, Long, Float>> elements, Collector<Tuple3<String, Long, Float>> out) {
            List<Tuple3<String, Long, Float>> list = new ArrayList<>();
            for (Tuple3<String, Long, Float> e : elements) list.add(e);
            list.sort((a, b) -> Float.compare(b.f2, a.f2)); // 按销售额降序
            for (int i = 0; i < Math.min(n, list.size()); i++) out.collect(list.get(i)); // 只输出前 n 条
        }
    }

    // TopN 函数(计数版): 用于任务7，按 Long 类型计数排序
    static class TopNProcessFunction2 extends
            ProcessWindowFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String, TimeWindow> {
        private final int n;
        public TopNProcessFunction2(int n) { this.n = n; }
        @Override
        public void process(String key, Context context,
                            Iterable<Tuple3<String, String, Long>> elements, Collector<Tuple3<String, String, Long>> out) {
            List<Tuple3<String, String, Long>> list = new ArrayList<>();
            for (Tuple3<String, String, Long> e : elements) list.add(e);
            list.sort((a, b) -> Long.compare(b.f2, a.f2)); // 按数量降序
            for (int i = 0; i < Math.min(n, list.size()); i++) out.collect(list.get(i));
        }
    }
}
