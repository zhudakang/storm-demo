package storm.demo.chapter1.v1;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;

/**
 * 业务代码
 * 实现语句分割bolt
 * BaseRichBolt 是一个简单的实现，继承这个类，就可以不用去实现本例子不需要关心的方法
 *
 * */
public class SplitSentenceBolt extends BaseRichBolt{
    private OutputCollector collector;

    /**
     *
     * 类同于Spout 中的open方法；
     * 用于初始化；
     * 初始化可以做一些例如初始化数据库连接等操作；
     * 不过这个例子没有额外的操作；
     * */
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 核心功能执行；
     * 从哪来的数据？从订阅中的数据流中，接收到一个tuple；
     * 然后这个方法读取sentence
     * 然后去根据空格分隔成为单词，然后在发射出去；
     * */
    public void execute(Tuple tuple) {
        String sentence = tuple.getStringByField("sentence");
        String[] words = sentence.split(" ");
        for(String word : words){
            this.collector.emit(new Values(word));
        }
    }

    /**
     * 声明输出流；
     * 这里面声明了输出流中包含了word这个字段；
     *
     * */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
