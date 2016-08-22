package org.jstorm;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class HelloWorldSpout extends BaseRichSpout {

	/**
	 * Hello world!
	 */
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private int referenceRandom;
	private static final int MAX_RANDOM = 10 ;
	
	public HelloWorldSpout(){
		final Random rand = new Random();
		referenceRandom = rand.nextInt(MAX_RANDOM);
	}
	
	/*
	 * nextTuple() => Storm cluster will repeatedly call the nextTuple method
	 * which will do all the work of the spout. nextTuple() must release the
	 * control of the thread when there is no work to do so that the other
	 * methods have a chance to be called.
	 */
	
	public void nextTuple() {

		final Random rand = new Random();
		int instanceRandom = rand.nextInt(MAX_RANDOM);
		if (instanceRandom == referenceRandom) {
			collector.emit(new Values("Hello World"));
		} else {
			collector.emit(new Values("Other Random Word"));
		}
	}

	/*
	 * open() => The first method called in any spout is 'open' TopologyContext
	 * => contains all our topology data SpoutOutputCollector => enables us to
	 * emit the data that will be processed by the bolts conf => created in the
	 * topology definition
	 */
	
	public void open(@SuppressWarnings("rawtypes")  Map conf, TopologyContext topologyContext, SpoutOutputCollector collector) {
		this.collector = collector;

	}

	/*
	 * declareOutputFields() => you need to tell the Storm cluster which fields
	 * this Spout emits within the declareOutputFields method.
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
