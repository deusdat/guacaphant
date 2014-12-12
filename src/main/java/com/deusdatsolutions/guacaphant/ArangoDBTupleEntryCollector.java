package com.deusdatsolutions.guacaphant;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapred.OutputCollector;

import com.arangodb.ArangoException;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntrySchemeCollector;

@SuppressWarnings("rawtypes")
public class ArangoDBTupleEntryCollector extends TupleEntrySchemeCollector 
implements OutputCollector<DriverContext, TupleEntry>{
	
	@SuppressWarnings("unchecked")
	public ArangoDBTupleEntryCollector(FlowProcess flowProcess, Scheme scheme) {
		super(flowProcess, scheme);
		this.setOutput(this);
	}

	@Override
	public void collect(DriverContext ctx, TupleEntry entry) throws IOException {
		//TODO Put batching logic in.
		Map<String, Object> doc = InteropTools.createMap(entry);
		try {
			ctx.getDriver().createDocument(ctx.getCollection(), doc);
		} catch (ArangoException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void close() {
		super.close();
		System.out.println("Cleanedup collector");
	}
}
