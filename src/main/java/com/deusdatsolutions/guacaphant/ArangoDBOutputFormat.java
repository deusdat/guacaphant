package com.deusdatsolutions.guacaphant;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import cascading.tuple.TupleEntry;

import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;


@SuppressWarnings("rawtypes")
public class ArangoDBOutputFormat implements OutputFormat, JobConfigurable {
	private static final Log	LOG	= LogFactory
											.getLog(ArangoDBOutputFormat.class);

	protected class ArangoDBRecordWriter implements
			RecordWriter<TupleEntry, Void> {
		private final ArangoDriver	conn;
		private final String		targetCollection;

		public ArangoDBRecordWriter(ArangoDriver connection,
				String targetCollection, boolean fireAndForget) {

			this.conn = connection;
			this.targetCollection = targetCollection;

			startBatch();
		}

		private void startBatch() {
		}

		@Override
		public void close(Reporter arg0) throws IOException {
			flushBatch(false);
		}

		@Override
		public void write(TupleEntry te, Void arg1) throws IOException {
			createDocument(te);
			flushBatch(true);
		}

		private void createDocument(TupleEntry te) throws IOException {
			Map<String, Object> jsonMap = InteropTools.createMap(te);
			try {
				conn.createDocument(targetCollection, jsonMap);
			} catch (ArangoException e) {
				throw new IOException(e);
			}
		}

		private void flushBatch(boolean restart) throws IOException {

		}
	}

	@Override
	public void checkOutputSpecs(FileSystem fs, JobConf arg1)
			throws IOException {
	}

	@Override
	public RecordWriter getRecordWriter(FileSystem fs, JobConf conf,
			String arg2, Progressable progress) throws IOException {
		LOG.info("Creating the ArangoDBRecordWriter.");

		ArangoDBConfiguration c = new ArangoDBConfiguration(conf);
		String targetCollection = c.getTargetCollection();
		ArangoDriver connection = c.connection();

		return new ArangoDBRecordWriter(connection, targetCollection, false);
	}

	@Override
	public void configure(JobConf conf) {

	}

}
