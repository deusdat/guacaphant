package com.deusdatsolutions.guacaphant;

import java.util.Map;

import cascading.tuple.TupleEntry;

public class ArangoDBWriter {
	private TupleEntry t;
	
	public ArangoDBWriter() {
		
	}
	
	void read(Map<String, Object> record) {
		t = InteropTools.createTupleEntry(record);
	}
	
	public TupleEntry getTupleEntry() {
		return t;
	}
}
