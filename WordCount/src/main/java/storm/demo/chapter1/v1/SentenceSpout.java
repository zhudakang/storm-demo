package storm.demo.chapter1.v1;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.Map;
import storm.demo.utils.Utils;

/**
 * BaseRichSpout相当于一个比较简单的实现
 *
 * */
public class SentenceSpout extends BaseRichSpout {

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
     * 但是这个地方因为open只是简单地将SpoutOutputCollector对象的引用保存在变量中
     * 保存在这个类中
     * */
    public void open(Map config, TopologyContext context, 
            SpoutOutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 核心实现；
     * storm通过这个方法向输出的collector发射tuple
     * 这个意思就是我们发射当前索引对应的语句，然后递增索引指向下一个语句
     * */
    public void nextTuple() {
        //这个sentences[index]相当于tuple，其实就是一个list
        //emit相当于发出发射的意思
        this.collector.emit(new Values(sentences[index]));
        index++;
        //这个应该是一个循环
        if (index >= sentences.length) {
            index = 0;
        }
        Utils.waitForMillis(1);
    }

    public static void main(String[] args) {
        String[] sentences = {
                "my dog has fleas",
                "i like cold beverages",
                "the dog ate my homework",
                "don't have a cow man",
                "i don't think i like fleas"
        };
        for (String str : sentences) {
            System.out.println(str);
        }
    }
}
