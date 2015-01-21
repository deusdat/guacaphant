package com.deusdatsolutions.guacaphant;

import java.util.Map;

import cascading.tuple.Tuple;

public class ArangoDBWriter {
	private Tuple t;
	
	public ArangoDBWriter() {
		
	}
	
	void read(Map<String, Object> record) {
		t = InteropTools.createTupleEntry(record).getTuple();
	}
	
	public Tuple getTuple() {
		return t;
	}
}
