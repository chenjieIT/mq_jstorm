package org.jstorm;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class DataProcesBolt extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(ProduceDataSpout.class);

    OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String xx = input.getString(0);
        int a=input.size();
        input.getDouble(a);
        LOGGER.info("aaa" ,xx);
        System.out.println(" [x] start........................."+xx);  
        // 发送ack信息告知spout 完成处理的消息 ，如果下面的hbase的注释代码打开了，则必须等到插入hbase完毕后才能发送ack信息，这段代码需要删除
        this.collector.ack(input);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } 
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }


}
