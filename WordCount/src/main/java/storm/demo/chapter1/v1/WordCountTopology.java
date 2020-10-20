package storm.demo.chapter1.v1;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import static storm.demo.utils.Utils.*;

/**
 * 业务代码
 * 实现拓扑；
 * 定义好了输入 和计算单元bolt进行整合成为一个可以运行的拓扑；
 * */
public class WordCountTopology {

    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    /**
     * 输出结果：
     * --- FINAL COUNTS ---
     * a : 1344
     * ate : 1344
     * beverages : 1344
     * cold : 1344
     * cow : 1344
     * dog : 2688
     * don't : 2687
     * fleas : 2687
     * has : 1344
     * have : 1344
     * homework : 1344
     * i : 4030
     * like : 2687
     * man : 1344
     * my : 2688
     * the : 1344
     * think : 1343
     * --------------
     *
     * */
    public static void main(String[] args) throws Exception {

        //输入
        SentenceSpout spout = new SentenceSpout();
        //bolt
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();


        //官方类，这个类用来提供流式的接口风格的API来定义拓扑组件之间的数据流
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SENTENCE_SPOUT_ID, spout);
        /**
         * 对于这个builder，我们首先注册一个bolt订阅
         * 使用.shuffleGrouping(SENTENCE_SPOUT_ID); 来订阅语句的数据源；
         * shuffleGrouping这个方法用来告诉storm，要将spout发射的tuple随机均匀的分发给SPLIT_BOLT
         * 对于setBolt这个方法会注册bolt,并且返回BoltDeclarer 这个实例；
         * 这边涉及到了"数据流分组"的内容；
         * */
        // SentenceSpout --> SplitSentenceBolt
        builder.setBolt(SPLIT_BOLT_ID, splitBolt)
                .shuffleGrouping(SENTENCE_SPOUT_ID);

        /**
         * 将特定的tuple路由到特殊的bolt实例中；
         * 重点：
         * 可以使用fieldsGrouping方法来保证所有的word字段值相同的tuple 会路由到同一个wordCountBolt中
         * */
        // SplitSentenceBolt --> WordCountBolt
        builder.setBolt(COUNT_BOLT_ID, countBolt)
                .fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));

        /**
         * globalGrouping 统一路由到唯一的ReportBolt的任务重
         *
         * */
        // WordCountBolt --> ReportBolt
        builder.setBolt(REPORT_BOLT_ID, reportBolt)
                .globalGrouping(COUNT_BOLT_ID);

        //上述数据流都已经定义好，运行单词计数计算的最后一步是编译并提交到集群上
        Config config = new Config();

        //这边使用的是Storm本地模式 LocalCluster在本地的开发环境中来模拟一个完整的storm集群
        LocalCluster cluster = new LocalCluster();

        //当一个拓扑提交的时候，storm会将默认配置和config实例中的配置合并然后作为参数传递给submitTopology
        //合并之后的配置将会分发给各个spout的bolt的open() prepare()方法
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        //控制执行的时长，修改这个会输出结果不一样
        waitForSeconds(10);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
