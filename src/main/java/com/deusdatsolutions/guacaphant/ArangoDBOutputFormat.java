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

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.arangodb.entity.DefaultEntity;


public class ArangoDBOutputFormat implements OutputFormat, JobConfigurable {
	private static final Log	LOG	= LogFactory
											.getLog(ArangoDBOutputFormat.class);

	protected class ArangoDBRecordWriter implements
			RecordWriter<TupleEntry, Void> {
		private final ArangoDriver	conn;
		private final int			batchSize;
		private long				sent;
		private final String		targetCollection;
		private final Fields		finalFields;

		public ArangoDBRecordWriter(ArangoDriver connection, int batchSize,
				String targetCollection, boolean fireAndForget,
				Fields finalFields) {

			this.conn = connection;
			this.batchSize = batchSize;
			this.targetCollection = targetCollection;
			this.finalFields = finalFields;

			startBatch();
		}

		private void startBatch() {
//			try {
//				conn.startBatchMode();
//				this.sent = 0;
//			} catch (ArangoException e) {
//				new IOException(e);
//			}
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
			this.sent++;
			Map<String, Object> jsonMap = InteropTools.createMap(te);
			try {
				conn.createDocument(targetCollection, jsonMap);
			} catch (ArangoException e) {
				throw new IOException(e);
			}
		}

		private void flushBatch(boolean restart) throws IOException {
//			try {
//				if (this.sent == this.batchSize || !restart) {
//					DefaultEntity executeBatch = conn.executeBatch();
//					
//					if (restart) {
//						startBatch();
//					}
//				}
//			} catch (ArangoException e) {
//				throw new IOException(e);
//			}
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
		int batchSize = /* c.getWriteBatchSize() */ 1;

		return new ArangoDBRecordWriter(connection, batchSize, targetCollection, false,
				Fields.ALL);
	}

	@Override
	public void configure(JobConf conf) {

	}

}
