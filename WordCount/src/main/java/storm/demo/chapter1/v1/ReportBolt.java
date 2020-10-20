package storm.demo.chapter1.v1;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 实现上报的BOLT
 * 对所有的单词计数生成一份报告；
 * 在这边是简单的将接收到的计数BOLT发射出的计数tuple 进行存储；
 * */
public class ReportBolt extends BaseRichBolt {

    private HashMap<String, Long> counts = null;

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.counts = new HashMap<String, Long>();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word, count);
    }

    /**
     * 数据流末端的bolt，不会发射tuple；
     * */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // this bolt does not emit anything
    }

    /**
     * stoorm在种植一个bolt之前会调用这个方法;
     * 通常用来释放bolt占用的资源,比如释放打开的句柄 或者数据库连接；
     * 重点：
     * 但是对于拓扑在storm集群上面运行的时候，这个cleanup方法是不可靠的；
     * 不能保证会执行，涉及到了strom的容错机制；
     * 如果是在开发环境中，是可以保证这个被调用的；
     * */
    @Override
    public void cleanup() {
        System.out.println("--- FINAL COUNTS ---");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            System.out.println(key + " : " + this.counts.get(key));
        }
        System.out.println("--------------");
    }
}
