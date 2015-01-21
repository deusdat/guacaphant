package com.deusdatsolutions.guacaphant;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import static com.deusdatsolutions.guacaphant.utils.MiscOps.*;

@SuppressWarnings("rawtypes")
public class ArangoDBTap extends Tap<JobConf, RecordReader, OutputCollector> {

	private static final long	serialVersionUID	= 6215877448790957829L;
	private String				username;
	private String				password;
	private String				baseUrl;

	public ArangoDBTap(final String baseUrl) {
		this.baseUrl = baseUrl;
	}

	public ArangoDBTap(final String baseUrl, final String username,
			final String password) {
		this(baseUrl);
		this.username = username;
		this.password = password;
	}

	@Override
	public String getIdentifier() {
		return null;
	}

	@Override
	public void sourceConfInit(FlowProcess<JobConf> flowProcess, JobConf conf) {
		FileInputFormat.setInputPaths(conf, this.getPath());
		if (has(username)) {
			ArangoDBConfiguration.set(conf, baseUrl, username, password);
		} else {
			ArangoDBConfiguration.set(conf, baseUrl);
		}
		
		super.sourceConfInit(flowProcess, conf);
	}

	private Path getPath() {
		return new Path(this.baseUrl);
	}

	@Override
	public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess,
			RecordReader input) throws IOException {
		return null;
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
