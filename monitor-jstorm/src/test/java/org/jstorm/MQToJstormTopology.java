package org.jstorm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用Maven 编译打包 HelloWorld.jar
 *  $JSTORM_HOME/bin/jstorm jar /home/god/helloWorldTopology.jar org.jstorm.HelloWorldTopology HelloWorld
 *  其中最后的参数[HelloWorld]为TopologyName
 */
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class MQToJstormTopology {

	private static Logger LOG = LoggerFactory.getLogger(MQToJstormTopology.class);
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		LOG.debug("test");
	    LOG.info("Submit log");
		TopologyBuilder builder = new TopologyBuilder();
		LocalCluster cluster = new LocalCluster();
		builder.setSpout("rabbitMqSpout", new RabbitMqSpout(), 1);
		builder.setBolt("jstormToMqBolt", new JstormToMqBolt(), 1).shuffleGrouping("rabbitMqSpout");
		Config conf = new Config();
		if (args != null && args.length > 0) {
			conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
			//提交拓扑
			cluster.submitTopology("SequenceTest", conf, builder.createTopology());
//			try {
//				Thread.sleep(10000);
//			} catch (InterruptedException e) {
//				LOG.error("Thread InterruptedException error.",e);
//			}		
//			//结束拓扑
//		 cluster.killTopology("SequenceTest");
//	     cluster.shutdown();
		}else{
			// JStorm 安装完后，默认的NIMBUS端口配置为7672
//		conf.put(Config.NIMBUS_THRIFT_PORT, 7672);
//		conf.put(Config.NIMBUS_HOST, "10.111.58.166");
			conf.setDebug(false);
			//建议加上这行，使得每个bolt/spout的并发度都为1
			conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 10);
			conf.setNumWorkers(3);
			// 如果在JStrom集群中运行
			//	StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}
			
		

	}
}