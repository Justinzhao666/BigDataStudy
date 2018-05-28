/**
 * WordCountStormTopology Create on 2018/5/28
 * Description:
 * <p>
 * author justin
 * version 1.0
 * Copyright (c) 2018/5/28 by justin
 */

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.shade.org.apache.commons.collections.map.HashedMap;
import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
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

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 使用storm完成词频统计功能
 */
public class WordCountStormTopology {

    /**
     * 定义spout，spout需要继承BaseRichSpout
     */
    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;

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
         * 业务：
         * 1.读取指定目录下的文件夹下的数据
         * 2.将每一行数据发射出去
         */
        public void nextTuple() {
            // 使用common.io包下面的FileUtils来操作文件
            Collection<File> files = FileUtils.listFiles(new File("/Users/justin/WorkSpace/Company/hcx/BigData/storm/src/main/resources/wc"), new String[]{"txt"}, true);
            files.forEach(file -> {
                try {
                    //获取文件中所有的内容
                    List<String> lines = FileUtils.readLines(file);
                    lines.forEach(line -> this.collector.emit(new Values(line)));
                    //将文件名修改，防止循环读取
                    FileUtils.moveFile(file, new File(file.getAbsolutePath() + System.currentTimeMillis()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        /**
         * 声明输出字段，这个spout传递的数据的field是num，bolt只要接受num字段的tuple
         *
         * @param declarer
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }


    /**
     * 对内容进行分割
     */
    public static class SplitBolt extends BaseRichBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        /**
         * bolt的业务:
         * 对line进行逗号分割
         * @param input
         */
        @Override
        public void execute(Tuple input) {
            String line = input.getStringByField("line");
            String[] words = line.split(",");
            for (String word : words) {
                this.collector.emit(new Values(word));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class CountBolt extends BaseRichBolt{

        Map<String,Integer> map = new HashMap();

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }

        /**
         * 业务：
         * 获取每个单词
         * 对单词进行汇总
         * 输出
         * @param input
         */
        @Override
        public void execute(Tuple input) {
            String word = input.getStringByField("word");
            Integer count = map.get(word);
            count = count == null ? 0: ++count;
            map.put(word,count);
            System.out.println(JSON.toString(map));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {
        //构建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout",new DataSourceSpout());
        builder.setBolt("SplitBolt",new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt",new CountBolt()).shuffleGrouping("SplitBolt");

        //创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(WordCountStormTopology.class.getName(),new Config(),builder.createTopology());
    }

}
