package com.deusdatsolutions.guacaphant;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;



/**
 * A tap for Cascading to get information in and out of ArangoDB.
 * @author J Patrick Davenport
 *
 */
@SuppressWarnings("rawtypes")
public class ArangoDBTap extends Tap<JobConf, RecordReader, OutputCollector>{
	private static final long serialVersionUID = 1L;
	private final ArangoDBScheme scheme;
	private final String aqlQuery;
	
	@SuppressWarnings("unchecked")
	public ArangoDBTap(ArangoDBScheme scheme, String aqlQuery) {
		super(scheme);
		this.scheme = scheme;
		this.aqlQuery = aqlQuery;
	}

	
	public ArangoDBTap(ArangoDBScheme scheme) {
		this(scheme, "");
	}

	@Override
	public boolean createResource(JobConf conf) throws IOException {
		// TODO Come back to the philosophy of this.
		return true;
	}

	@Override
	public boolean deleteResource(JobConf arg0) throws IOException {
		// TODO Come back to the philosophy of this.
		return false;
	}

	@Override
	public String getIdentifier() {
		return scheme.getPath() + aqlQuery;
	}

	@Override
	public long getModifiedTime(JobConf arg0) throws IOException {
		return System.currentTimeMillis();
	}

	@Override
	public TupleEntryIterator openForRead(FlowProcess<JobConf> conf,
			RecordReader reader) throws IOException {
		return new HadoopTupleEntrySchemeIterator(conf, this, reader);
	}

	@Override
	public TupleEntryCollector openForWrite(FlowProcess<JobConf> conf,
			OutputCollector outputCollector) throws IOException {
		return new ArangoDBTupleEntryCollector(conf, this.getScheme());
	}

	@Override
	public boolean resourceExists(JobConf conf) throws IOException {
		return true;
	}
	
	@Override
	public boolean equals(Object object) {
		if(object == null || !(object instanceof ArangoDBTap)) {
			return false;
		}
		
		ArangoDBTap t = (ArangoDBTap) object;
		return t.getIdentifier().equals(this.getIdentifier());
	}
	
	@Override
	public int hashCode() {
		int result = 31 * super.hashCode();
		return  result + this.getIdentifier().hashCode();
	}

}
