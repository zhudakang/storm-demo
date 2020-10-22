package storm.demo.chapter1.group;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

/**
 * @desc: MyGrouping
 * @author: zhudakang@baijiahulian.com
 * @date: 2020/10/22 4:55 下午
 */
public class CustomGrouping implements CustomStreamGrouping {

    //接受目标任务的id集合
    private List<Integer> targetTasks ;

    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks ;
    }

    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> subTaskIds = new ArrayList<Integer>();
        for(int i = 0 ; i <= targetTasks.size() / 2 ; i ++){
            subTaskIds.add(targetTasks.get(i));
        }
        return subTaskIds;
    }
}
