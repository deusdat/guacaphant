package com.deusdatsolutions.guacaphant;

import com.arangodb.ArangoDriver;

public class DriverContext {
	private final ArangoDriver driver;
	private final String collection;
	private final boolean createCollection;
	
	public DriverContext(ArangoDriver driver, String collection, boolean createCollection) {
		super();
		this.driver = driver;
		this.collection = collection;
		this.createCollection = createCollection;
	}

	public boolean isCreateCollection() {
		return createCollection;
	}

	public ArangoDriver getDriver() {
		return driver;
	}

	public String getCollection() {
		return collection;
	}
}
