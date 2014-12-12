package com.deusdatsolutions.guacaphant;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import com.arangodb.ArangoConfigure;
import com.arangodb.ArangoDriver;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class ArangoDBScheme extends Scheme {
	private final String host;
	private final Integer port;
	private final String username;
	private final String password;
	private final String database;
	private final String collection;
	private final boolean createCollection;

	private transient ArangoDriver driver;
	private transient DriverContext ctx;

	public ArangoDBScheme(String host, Integer port, String username,
			String password, String database, String collection, boolean createCollection) {

		super();
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.database = database;
		this.collection = collection;
		this.createCollection = createCollection;
		init();
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		init();
	}

	private void init() {
		ArangoConfigure conf = new ArangoConfigure();
		if (this.host != null)
			conf.setHost(host);
		if (this.database != null)
			conf.setDefaultDatabase(database);
		if (port != null)
			conf.setPort(port);
		if (username != null)
			conf.setUser(username);
		if (password != null)
			conf.setPassword(password);
		if (collection != null)
			conf.init();
		driver = new ArangoDriver(conf);
		
		ctx = new DriverContext(driver, collection, createCollection);
	}

	protected String getPath() {
		return host + port + database + collection;
	}

	@Override
	public void sourceConfInit(FlowProcess flowProcess, Tap tap, Object conf) {

	}

	@SuppressWarnings("unchecked")
	@Override
	public void sinkCleanup(FlowProcess flowProcess, SinkCall sinkCall)
			throws IOException {
		super.sinkCleanup(flowProcess, sinkCall);
	}

	@Override
	public void sinkConfInit(FlowProcess flowProcess, Tap tap, Object conf) {

	}

	@Override
	public boolean source(FlowProcess flowProcess, SourceCall sourceCall)
			throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void sink(FlowProcess flowProcess, SinkCall sinkCall)
			throws IOException {
		TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
		ArangoDBTupleEntryCollector outputCollector = (ArangoDBTupleEntryCollector) sinkCall
				.getOutput();

		outputCollector.collect(ctx, tupleEntry);
	}

}
