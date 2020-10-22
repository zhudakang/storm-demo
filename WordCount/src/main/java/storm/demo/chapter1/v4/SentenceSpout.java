package storm.demo.chapter1.v4;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import storm.demo.utils.Utils;


/**
 *
 * 增强SentenceSpout 类，支持可靠的tuple发射方式。
 * 需要记录所有发送的tuple，有点像链路的感觉？
 * 分配一个唯一标识符
 * */
public class SentenceSpout extends BaseRichSpout {


    //存储已发送待确认的tuple 每当发送一个新的tuple，分配一个唯一的标识符并且存储在我们的hashMap中，收到确认消息之后，从
    //待确认列表删除该tuple，如果收到报错 从新发送
    private ConcurrentHashMap<UUID, Values> pending;

    private SpoutOutputCollector collector;

    private String[] sentences = {
        "my dog has fleas",
        "i like cold beverages",
        "the dog ate my homework",
        "don't have a cow man",
        "i don't think i like fleas"
    };
    private int index = 0;

    /**
     * 所有的storm的组件都应该实现这个接口
     * 通过这个方法会告诉storm 这个组件这个类将会发射那些数据流
     * 就是发射sentence这个数据流
     * 这边声明的数据流，在下面算子中，也就是SplitSentenceBolt 会去获取这个值；
     * */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    /**
     * 所有的spout的组件在初始化的时候调用这个方法
     * map是包含了Storm的配置信息的map，第二个TopologyContext对象提供了拓扑中组件的信息，SpoutOutputCollector提供了发射tuple的方法。
     * 这边的点在于 有个pending的初始化
     *
     * */
    public void open(Map config, TopologyContext context,
            SpoutOutputCollector collector) {
        this.collector = collector;
        this.pending = new ConcurrentHashMap<UUID, Values>();
    }

    public void nextTuple() {
        Values values = new Values(sentences[index]);
        UUID msgId = UUID.randomUUID();
        //这边会把values 对应的语句放到map里面，ID是一个随机数
        this.pending.put(msgId, values);
        this.collector.emit(values, msgId);
        index++;
        if (index >= sentences.length) {
            index = 0;
        }
        Utils.waitForMillis(1);
    }

    public void ack(Object msgId) {
        this.pending.remove(msgId);
    }

    //失败重发
    public void fail(Object msgId) {
        this.collector.emit(this.pending.get(msgId), msgId);
    }
}
