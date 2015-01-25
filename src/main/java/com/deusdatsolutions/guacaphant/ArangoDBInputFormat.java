package com.deusdatsolutions.guacaphant;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.arangodb.CursorResultSet;
import com.arangodb.entity.CursorEntity;

public class ArangoDBInputFormat implements
		InputFormat<LongWritable, ArangoDBWriter>, JobConfigurable {
	private static final Logger LOG = LoggerFactory.getLogger(ArangoDBInputFormat.class);
	
	
	protected ArangoDBConfiguration	dbConfig;
	private int						reads;
	private String					mainQuery;
	private String					returnStatement;
	private String					sortStatement;

	@Override
	public RecordReader<LongWritable, ArangoDBWriter> getRecordReader(
			InputSplit split, JobConf arg1, Reporter arg2) throws IOException {
		return new ArangoDBRecordReader((ArangoDBSplit) split, arg1);
	}

	@Override
	public InputSplit[] getSplits(JobConf conf, int chunks) throws IOException {
		int numSplits = reads == 0 ? chunks : reads;

		ArangoDriver connection = dbConfig.connection();
		long offset = findSplitSize(connection, numSplits);

		InputSplit[] splits = new InputSplit[numSplits];
		boolean fromSplit = numSplits > 1;
		for (int i = 0; i < numSplits; i++) {
			ArangoDBSplit split = new ArangoDBSplit(i * offset, offset,
					fromSplit);
			splits[i] = split;
		}
		return splits;
	}

	protected long findSplitSize(ArangoDriver connection, int numSplits)
			throws IOException {
		try {
			String createCountQuery = createCountQuery();
			@SuppressWarnings("rawtypes")
			CursorEntity<Map> countCursor = connection.executeQuery(
					createCountQuery, null, Map.class, true, 1);
			int queryCount = countCursor.getCount();
			long splitSize = (queryCount / numSplits);
			return splitSize;
		} catch (ArangoException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void configure(JobConf conf) {
		dbConfig = new ArangoDBConfiguration(conf);
		this.reads = dbConfig.getPartitions();
		this.mainQuery = dbConfig.getMainQuery();
		this.returnStatement = dbConfig.getReturnStatement();
		this.sortStatement = dbConfig.getSortStatement();
	}

	private String createCountQuery() {
		StringBuilder sb = new StringBuilder();
		sb.append(mainQuery);
		sb.append("\n");
		sb.append("RETURN {}");
		return sb.toString();
	}

	@SuppressWarnings("rawtypes")
	protected class ArangoDBRecordReader implements
			RecordReader<LongWritable, ArangoDBWriter> {
		private ArangoDBSplit	split;
		private long pos;
		
		private CursorResultSet<Map> results;
		
		public ArangoDBRecordReader(ArangoDBSplit split, JobConf conf) throws IOException {
			super();
			this.split = split;
			ArangoDBConfiguration aConf = new ArangoDBConfiguration(conf);
			ArangoDriver connection = aConf.connection();
			try {
				 String aql = getAQL();
				 LOG.debug("Querying {}", aql);
				results = connection.executeQueryWithResultSet(aql, null, Map.class, false, 100);
			} catch (ArangoException e) {
				throw new IOException(e);
			}
		}

		protected String getAQL() {
			StringBuilder sb = new StringBuilder();
			sb.append(mainQuery);
			sb.append("\n");
			if(split.isFromSplit()) {
				sb.append("SORT ");
				sb.append(sortStatement);
				sb.append("\n");
				sb.append("LIMIT ");
				sb.append(Long.toString(split.getStart()));
				sb.append(",");
				sb.append(Long.toString(split.getOffset()));
				sb.append("\n");
			}
			sb.append(returnStatement);
			
			return sb.toString();
		}

		@Override
		public void close() throws IOException {
			try {
				results.close();
			} catch (ArangoException e) {
				throw new IOException();
			}
		}

		@Override
		public LongWritable createKey() {
			return new LongWritable();
		}

		@Override
		public ArangoDBWriter createValue() {
			return new ArangoDBWriter();
		}

		@Override
		public long getPos() throws IOException {
			return pos;
		}

		@Override
		public float getProgress() throws IOException {
			return pos/split.getLength();
		}

		@SuppressWarnings("unchecked")
		@Override
		public boolean next(LongWritable key, ArangoDBWriter value)
				throws IOException {
			if(!results.hasNext()) {
				return false;
			}
			key.set(pos + split.getStart());
			value.read(results.next());
			pos++;
			return true;
		}

	}

	protected static class ArangoDBSplit implements InputSplit {
		private long	start;
		private long	offset;
		private boolean	fromSplit;

		public ArangoDBSplit() {}
		
		public ArangoDBSplit(long start, long offset, boolean fromSplit) {
			super();
			this.start = start;
			this.offset = offset;
			this.fromSplit = fromSplit;
		}

		@Override
		public void readFields(DataInput storage) throws IOException {
			this.offset = storage.readLong();
			this.start = storage.readLong();
			this.fromSplit = storage.readBoolean();
		}

		@Override
		public void write(DataOutput storage) throws IOException {
			storage.writeLong(offset);
			storage.writeLong(start);
			storage.writeBoolean(fromSplit);
		}

		@Override
		public long getLength() throws IOException {
			return start + offset;
		}

		public long getStart() {
			return start;
		}

		public long getOffset() {
			return offset;
		}

		public boolean isFromSplit() {
			return fromSplit;
		}

		@Override
		public String[] getLocations() throws IOException {
			return new String[]{};
		}

	}
}
