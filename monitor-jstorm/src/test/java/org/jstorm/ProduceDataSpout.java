package org.jstorm;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;;

public class ProduceDataSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(ProduceDataSpout.class);
	static AtomicInteger sAtomicInteger = new AtomicInteger(0);
	static AtomicInteger pendNum = new AtomicInteger(0);
	private int sqnum;
	SpoutOutputCollector collector;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		sqnum = sAtomicInteger.incrementAndGet();
		this.collector = collector;
	}

	public void nextTuple() {
		while (true) {
			int a = pendNum.incrementAndGet();
			LOGGER.info(String.valueOf(sqnum),String.valueOf(a));
			this.collector.emit(new Values(a,a*10));

			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("log"));

	}

	@Override
	public void ack(Object msgId) {
		super.ack(msgId);
	}

	/**
	 * 消息处理失败后需要自己处理
	 * 
	 * @param msgId
	 */
	@Override
	public void fail(Object msgId) {
		super.fail(msgId);
		LOGGER.info("ack fail,msgId" + msgId);
	}

}
