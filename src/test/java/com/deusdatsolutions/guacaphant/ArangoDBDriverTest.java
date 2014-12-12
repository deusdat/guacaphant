package com.deusdatsolutions.guacaphant;

import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.collections4.map.HashedMap;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.arangodb.ArangoConfigure;
import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.arangodb.CursorResultSet;

public class ArangoDBDriverTest extends TestCase {
	public void testShouldInsertMap() throws ArangoException {
		ArangoConfigure conf = new ArangoConfigure();
		conf.setHost("192.168.56.10");
		conf.setDefaultDatabase("Experimenting");
		conf.init();

		ArangoDriver arangoDriver = new ArangoDriver(conf);
//		arangoDriver.startBatchMode();
//		{
//			Map<String, Object> doc = new HashedMap<>();
//			doc.put("upc", "12345qwewqe67890");
//			arangoDriver.createDocument("DriverTest", doc);
//		}
//		
//		{
//			Map<String, Object> doc = new HashedMap<>();
//			doc.put("upc", "sadasdads");
//			arangoDriver.createDocument("DriverTest", doc);
//		}
//		
//		arangoDriver.executeBatch();
		
		TupleEntry teOutter = new TupleEntry(new Fields("upc", "id", "manufacture"), Tuple.size(3));
		TupleEntry teMan = new TupleEntry(new Fields("address", "id"), Tuple.size(2));
		
		String appliedAddress = "1234 Fake St.";
		teMan.setString(new Fields("address"), appliedAddress);
		
		teOutter.setString("upc", "1234");
		teOutter.setString("id", "id");
		teOutter.setObject("manufacture", teMan);
		
		arangoDriver.createDocument("DriverTest", InteropTools.createMap(teOutter));
		
		CursorResultSet rs = arangoDriver.executeQueryWithResultSet("FOR dts IN DriverTest RETURN dts", (Map)null, Map.class, false, 10);
		Map<String, Object> first = (Map<String, Object>) rs.next();
		System.out.println(first.get("_id"));
	}
}
