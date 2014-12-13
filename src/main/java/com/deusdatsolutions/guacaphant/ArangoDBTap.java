package com.deusdatsolutions.guacaphant;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import com.arangodb.ArangoException;
import com.arangodb.CursorResultSet;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeIterator;

/**
 * A tap for Cascading to get information in and out of ArangoDB.
 * 
 * @author J Patrick Davenport
 *
 */
@SuppressWarnings("rawtypes")
public class ArangoDBTap extends Tap<JobConf, RecordReader, OutputCollector> {
    private static final long serialVersionUID = 1L;
    private final ArangoDBScheme scheme;

    @SuppressWarnings("unchecked")
    public ArangoDBTap(ArangoDBScheme scheme) {
	super(scheme);
	this.scheme = scheme;
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
	return scheme.getPath();
    }

    @Override
    public long getModifiedTime(JobConf arg0) throws IOException {
	return System.currentTimeMillis();
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<JobConf> conf,
	    RecordReader reader) throws IOException {
	final CursorResultSet<Map<String, Object>> cursor = scheme
		.executeQuery();
	return new TupleEntrySchemeIterator(conf,
		getScheme(), new Closeable() {

		    @Override
		    public void close() throws IOException {
			try {
			    cursor.close();
			} catch (ArangoException e) {
			    throw new IOException(e);
			}
		    }
		});
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
	if (object == null || !(object instanceof ArangoDBTap)) {
	    return false;
	}

	ArangoDBTap t = (ArangoDBTap) object;
	return t.getIdentifier().equals(this.getIdentifier());
    }

    @Override
    public int hashCode() {
	int result = 31 * super.hashCode();
	return result + this.getIdentifier().hashCode();
    }

}
