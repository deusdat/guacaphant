package com.deusdatsolutions.guacaphant;

import static com.deusdatsolutions.guacaphant.utils.MiscOps.has;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.arangodb.entity.CollectionEntity;

@SuppressWarnings("rawtypes")
public class ArangoDBTap extends Tap<JobConf, RecordReader, OutputCollector> {

	private static final long				serialVersionUID	= 6215877448790957829L;
	private String							username;
	private String							password;
	private String							server;
	private int								port;
	private boolean							replace;
	private ArangoDBScheme					scheme;
	private final String					id;
	private transient ArangoDBConfiguration	c;

	public ArangoDBTap(ArangoDBScheme scheme, final String server,
			SinkMode sinkMode) {
		this(scheme, server, false, sinkMode);
	}

	public ArangoDBTap(ArangoDBScheme scheme, final String server,
			final int port, final String username, final String password,
			SinkMode sinkMode) {
		this(scheme, server, sinkMode);
		this.port = port;
		this.username = username;
		this.password = password;
	}

	public ArangoDBTap(final ArangoDBScheme scheme, final String server,
			final boolean replace, SinkMode sinkMode) {
		super(scheme, sinkMode);
		this.id = UUID.randomUUID().toString();
		this.server = server;
		this.port = 8529;
		this.scheme = scheme;
		this.replace = replace;
	}

	@Override
	public String getIdentifier() {
		return getEndpoint() + id;
	}

	@Override
	public void sourceConfInit(FlowProcess<JobConf> flowProcess, JobConf conf) {
		FileInputFormat.setInputPaths(conf, this.getPath());
		configArango(conf);

		super.sourceConfInit(flowProcess, conf);
	}

	@Override
	public void sinkConfInit(FlowProcess<JobConf> flowProcess, JobConf conf) {
		if (!isSink())
			return;
		super.sinkConfInit(flowProcess, conf);
		configArango(conf);
	}

	private void configArango(JobConf conf) {
		String targetCollection = scheme.getTargetCollection();
		if (has(username)) {
			ArangoDBConfiguration.set(conf, server, port, username, password,
					targetCollection);
		} else {
			ArangoDBConfiguration.set(conf, server, port, null, null,
					targetCollection);
		}
		c = new ArangoDBConfiguration(conf);
	}

	private Path getPath() {
		return new Path(getEndpoint());
	}

	private String getEndpoint() {
		return "http://" + this.server + ":" + port + "/"
				+ scheme.getDatabase();
	}

	@Override
	public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess,
			RecordReader input) throws IOException {
		return new HadoopTupleEntrySchemeIterator(flowProcess, this, input);
	}

	@Override
	public TupleEntryCollector openForWrite(
			FlowProcess<JobConf> TupleEntrySchemeCollector,
			OutputCollector output) throws IOException {
		return new ArangoDBTapCollector(TupleEntrySchemeCollector, this);
	}

	@Override
	public boolean createResource(JobConf conf) throws IOException {
		boolean result = false;
		System.out.println("Attempting to configure output");
		ArangoDriver driver = c.connection();
		String targetCollection = c.getTargetCollection();
		try {
			CollectionEntity collection = driver
					.getCollection(targetCollection);
			if (collection.isNotFound())
				driver.createCollection(targetCollection);
			else
				result = true;
		} catch (ArangoException e) {
			if(e.getErrorNumber() == 1203) {
				try {
					driver.createCollection(targetCollection);
					result = true;
				} catch (ArangoException e1) {
					throw new IOException(e1);
				}
			} else {
				throw new IOException(e);
			}
		}
		return result;
	}

	@Override
	public boolean deleteResource(JobConf conf) throws IOException {
		if (!isSink()) {
			return false;
		}

		try {
			String targetCollection = c.getTargetCollection();
			c.connection().deleteCollection(targetCollection);
			if (isReplace()) {
				c.connection().createCollection(targetCollection);
			}
			return true;
		} catch (ArangoException e) {
			return false;
		}
	}

	@Override
	public boolean resourceExists(JobConf conf) throws IOException {
		return createResource(conf);
	}

	@Override
	public long getModifiedTime(JobConf conf) throws IOException {
		return System.currentTimeMillis();
	}

	@Override
	public boolean isSink() {
		return scheme.getTargetCollection() != null;
	}

}
