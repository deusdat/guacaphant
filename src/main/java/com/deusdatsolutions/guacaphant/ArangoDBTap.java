package com.deusdatsolutions.guacaphant;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import static com.deusdatsolutions.guacaphant.utils.MiscOps.*;

@SuppressWarnings("rawtypes")
public class ArangoDBTap extends Tap<JobConf, RecordReader, OutputCollector> {

	private static final long	serialVersionUID	= 6215877448790957829L;
	private String				username;
	private String				password;
	private String				server;
	private int					port;
	private ArangoDBScheme		scheme;
	private final String		id;

	public ArangoDBTap(ArangoDBScheme scheme, final String server) {
		super(scheme);
		this.id = UUID.randomUUID().toString();
		this.server = server;
		this.port = 8529;
		this.scheme = scheme;
	}

	public ArangoDBTap(ArangoDBScheme scheme, final String server,
			final int port, final String username, final String password) {
		this(scheme, server);
		this.port = port;
		this.username = username;
		this.password = password;
	}

	@Override
	public String getIdentifier() {
		return getEndpoint() + id;
	}

	@Override
	public void sourceConfInit(FlowProcess<JobConf> flowProcess, JobConf conf) {
		FileInputFormat.setInputPaths(conf, this.getPath());
		if (has(username)) {
			ArangoDBConfiguration.set(conf, server, port, username, password);
		} else {
			ArangoDBConfiguration.set(conf, server, port);
		}

		super.sourceConfInit(flowProcess, conf);
	}

	private Path getPath() {
		return new Path(getEndpoint());
	}
	
	private String getEndpoint() {
		return "http://" + this.server + ":" + port + ":" + scheme.getDatabase();
	}

	@Override
	public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess,
			RecordReader input) throws IOException {
		return new HadoopTupleEntrySchemeIterator(flowProcess, this, input);
	}

	@Override
	public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess,
			OutputCollector output) throws IOException {
		return null;
	}

	@Override
	public boolean createResource(JobConf conf) throws IOException {
		return false;
	}

	@Override
	public boolean deleteResource(JobConf conf) throws IOException {
		return false;
	}

	@Override
	public boolean resourceExists(JobConf conf) throws IOException {
		return false;
	}

	@Override
	public long getModifiedTime(JobConf conf) throws IOException {
		return 0;
	}

}
