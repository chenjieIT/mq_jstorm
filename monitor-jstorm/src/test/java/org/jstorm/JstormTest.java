package org.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;


public class JstormTest {

	public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        LocalCluster cluster = new LocalCluster();
        builder.setSpout("testspout", new ProduceDataSpout(), 1);
        builder.setBolt("testbolt", new DataProcesBolt(), 2).shuffleGrouping("testspout");
        Config conf = new Config();
    	conf.setDebug(true);
		//建议加上这行，使得每个bolt/spout的并发度都为1
		conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
		conf.setNumWorkers(1);
        //			StormSubmitter.submitTopology("20160704", conf, builder.createTopology());
		cluster.submitTopology("SequenceTest", conf, builder.createTopology());
        System.out.println("storm cluster will start");
    	//结束拓扑
		 cluster.killTopology("SequenceTest");
	     cluster.shutdown();
	}

}
