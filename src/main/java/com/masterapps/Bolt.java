package com.masterapps;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class Bolt extends BaseRichBolt{
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		int number = input.getInteger(0);
		if( isPrime(number)){
			System.out.println(number);
		}
		collector.ack(input);
	}

	private boolean isPrime( int number ) {
		// TODO Auto-generated method stub
		if( number==1 || number==2 || number==3 ){
			return true;
		}
		
		if( number%2 == 0){
			return false;
		}
		
		for( int i=3 ; i*i<number ; i+=2){
			if(number%i == 0){
				return false;
			}
		}
		
		return true;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("number"));
	}

}
