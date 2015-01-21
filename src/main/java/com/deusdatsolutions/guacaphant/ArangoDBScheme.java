package com.deusdatsolutions.guacaphant;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;

@SuppressWarnings("unchecked")
/**
 * The scheme used to interact with ArangoDB's query API. 
 * 
 * The mainQuery is the bulk of the query with the actual logic in it.
 * <p>For example: mainQuery = "FOR u IN users FILTER u.name == 'Bob'"</p>
 * 
 * The returnClause is the return statement
 * <p>For example: RETURN u</p>
 * 
 * The sortClause is the snippet of a sort statement for the results. This is
 * necessary when the split size is designated.
 * <p>For example: u.last_name</p>
 * 
 * 
 * @author jdavenpo
 *
 */
public class ArangoDBScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]>{
	private static final long	serialVersionUID	= 1L;
	private String database;
	private String mainQuery;
	private String sortClause;
	private int concurrentReads;
	
	@Override
	public void sourceConfInit(FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
		conf.setInputFormat(ArangoDBInputFormat.class);
		ArangoDBConfiguration c = new ArangoDBConfiguration(conf);
		c.setDatabase(database);
		c.setMainQuery(mainQuery);
		c.setSortStatement(sortClause);
		c.setPartitions(concurrentReads);
	}

	@Override
	public void sinkConfInit(FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
	}

	@Override
	public boolean source(FlowProcess<JobConf> flowProcess,
			SourceCall<Object[], RecordReader> sourceCall) throws IOException {
		Object key = sourceCall.getContext()[0];
		Object value = sourceCall.getContext()[1];
		boolean result =sourceCall.getInput().next(key, value);
		
		if(!result) {
			return false;
		}
		
		Tuple t = ((ArangoDBWriter)value).getTuple();
		sourceCall.getIncomingEntry().setTuple(t);
		return true;
	}

	@Override
	public void sink(FlowProcess<JobConf> flowProcess,
			SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
	}

}
