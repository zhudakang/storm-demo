package storm.demo.chapter1.v1;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;

/**
 * 业务代码
 * 实现单词计数bolt
 * */
public class WordCountBolt extends BaseRichBolt{
    private OutputCollector collector;
    private HashMap<String, Long> counts = null;

    /**
     * 初始化；
     * 初始化counts  这是一个map，用来存储单词和对应的计数；
     * 在我们storm应用中其实很多也是各种维度分隔计数；
     * 重点：
     * 通常最好是在构造函数中对基本数据类型和可序列话的 对象进行复制和实例化；
     * prepare进行不可序列化的对象进行实例化;
     * HashMap 是可以序列化的；
     * */
    public void prepare(Map config, TopologyContext context, 
            OutputCollector collector) {
        this.collector = collector;
        this.counts = new HashMap<String, Long>();
    }

    /**
     * 获得单词 ;
     * 然后获取单词的数量；
     * 进行单词数量++；
     * 重新刷新发射
     * */
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = this.counts.get(word);
        if(count == null){
            count = 0L;
        }
        count++;
        this.counts.put(word, count);
        this.collector.emit(new Values(word, count));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
