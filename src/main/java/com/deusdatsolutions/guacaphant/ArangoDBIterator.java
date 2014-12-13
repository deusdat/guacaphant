package com.deusdatsolutions.guacaphant;

import java.io.IOException;
import java.util.Map;

import cascading.tuple.TupleEntry;
import cascading.util.CloseableIterator;

import com.arangodb.ArangoException;
import com.arangodb.CursorResultSet;

public class ArangoDBIterator implements CloseableIterator<Map<String, Object>> {
    private final CursorResultSet<Map<String, Object>> cursor;
    
    public ArangoDBIterator(CursorResultSet<Map<String, Object>> cursor) {
	this.cursor = cursor;
    }
    
    @Override
    public boolean hasNext() {
	boolean hasNext = this.cursor.hasNext();
	return hasNext;
    }

    @Override
    public Map<String, Object> next() {
	Map<String, Object> next = this.cursor.next();
	return next;
    }

    @Override
    public void close() throws IOException {
	try {
	    cursor.close();
	} catch (ArangoException e) {
	    throw new IOException(e);
	}
    }

}
