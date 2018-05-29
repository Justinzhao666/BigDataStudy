import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * LocalSumStormTopology Create on 2018/5/28
 * Description:
 *
 * author justin
 * version 1.0
 * Copyright (c) 2018/5/28 by justin
 */

public class ClusterSumStormTopology {

    /**
     * 定义spout，spout需要继承BaseRichSpout
     */
    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;
        int number = 0;

        /**
         * 初始化方法，只会被调用一次
         *
         * @param conf      配置参数
         * @param context   上下文
         * @param collector 数据发射器，暴露一个接口emit 将数据发射出去
         */
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        /**
         * 是个循环执行的方法，会不停的执行，从消息队列中取数据
         */
        public void nextTuple() {
            // 我们一般使用Values这个对象来包装传递的数据，并通过collector发射给bolt
            this.collector.emit(new Values(++number));
            System.out.println("Spout: " + number);
            // 防止数据产生太快
            Utils.sleep(1000);
        }

        /**
         * 声明输出字段，这个spout传递的数据的field是num，bolt只要接受num字段的tuple
         *
         * @param declarer
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("num"));
        }
    }


    /**
     * 数据累计求和bolt： 接受数据并处理
     */
    public static class SumBolt extends BaseRichBolt {

        int sum = 0;

        /**
         * 初始化方法，会被执行一次
         *
         * @param stormConf
         * @param context
         * @param collector
         */
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }

        /**
         * 获取spout发送过来的数据，死循环
         *
         * @param input
         */
        public void execute(Tuple input) {
            //使用spout设置的field值来获取数据
            Integer value = input.getIntegerByField("num");
            sum += value;
            System.out.println("Bolt: sum = [" + sum + "]");
        }

        /**
         * @param declarer
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }


    public static void main(String[] args) throws InvalidTopologyException {
        // 创建一个拓扑需要通过TopologyBuilder来创建
        TopologyBuilder builder = new TopologyBuilder();
        // 1. 将spout 和bolt的组件都加起来
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        // 2. 需要知道bolt和spout之前上下游的关系，使用shuffleGrouping
        builder.setBolt("SumBolt", new SumBolt()).shuffleGrouping("DataSourceSpout");

        //提交到storm集群上
        try {
            StormSubmitter.submitTopology("ClusterSumStormTopology",new Config(),builder.createTopology());
        } catch (AlreadyAliveException | AuthorizationException e) {
            e.printStackTrace();
        }
    }

}
