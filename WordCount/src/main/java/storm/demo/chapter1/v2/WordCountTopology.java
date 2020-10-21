package storm.demo.chapter1.v2;

import storm.demo.chapter1.v1.*;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import static storm.demo.utils.Utils.*;

public class WordCountTopology {

    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws Exception {

        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();


        TopologyBuilder builder = new TopologyBuilder();

        //增加线程数量，这个线程的数量不跟java线程一样，我们线上服务甚至开到了20个线程数，因为给的少更差性能
        builder.setSpout(SENTENCE_SPOUT_ID, spout, 2);
        // SentenceSpout --> SplitSentenceBolt
        builder.setBolt(SPLIT_BOLT_ID, splitBolt, 2).setNumTasks(4)
                .shuffleGrouping(SENTENCE_SPOUT_ID);
        // SplitSentenceBolt --> WordCountBolt
        builder.setBolt(COUNT_BOLT_ID, countBolt, 4)
                .fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        // WordCountBolt --> ReportBolt
        builder.setBolt(REPORT_BOLT_ID, reportBolt)
                .globalGrouping(COUNT_BOLT_ID);

        Config config = new Config();
        //增加worker的数量
        config.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        waitForSeconds(10);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }

    /**
     * 语句的hint 分隔的hint 计数的hint的对应数量
     * 是 2 2 4
     *
     * 结果：a : 2414
     * ate : 2411
     * beverages : 2412
     * cold : 2412
     * cow : 2415
     * dog : 4823
     * don't : 4824
     * fleas : 4829
     * has : 2412
     * have : 2412
     * homework : 2411
     * i : 7240
     * like : 4827
     * man : 2412
     * my : 4823
     * the : 2415
     * think : 2411
     *
     * 如果hint给到16 16  16的话，我电脑是2c4t ,好像都没有跑起来
     *如果给到4 4 8
     *
     * --- FINAL COUNTS ---
     * a : 5061
     * ate : 5052
     * beverages : 5053
     * cold : 5053
     * cow : 5062
     * dog : 10107
     * don't : 10103
     * fleas : 10121
     * has : 5054
     * have : 5052
     * homework : 5053
     * i : 15180
     * like : 10121
     * man : 5051
     * my : 10107
     * the : 5064
     * think : 5050
     * --------------
     *
     *
     * 如果给到12 12 24 都么有结果 还停不下来
     * */



}
