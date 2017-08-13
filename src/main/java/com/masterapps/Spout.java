package com.masterapps;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class Spout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private static int currentNumber = 1;
	

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		collector.emit(new Values(new Integer(currentNumber++)));
	}
	
	public void ack(Object id){};
	
	public void fail(Object id){};

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("number"));
		
	}
	

}
