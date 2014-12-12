package com.deusdatsolutions.guacaphant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class TupleEntryTest extends TestCase {
	public void testTEWithTe() {
		TupleEntry teOutter = new TupleEntry(new Fields("upc", "id", "manufacture"), Tuple.size(3));
		TupleEntry teMan = new TupleEntry(new Fields("address", "id"), Tuple.size(2));
		
		String appliedAddress = "1234 Fake St.";
		teMan.setString(new Fields("address"), appliedAddress);
		
		teOutter.setString("upc", "1234");
		teOutter.setString("id", "id");
		teOutter.setObject("manufacture", teMan);
		
		String addressValue = ((TupleEntry) (teOutter.getObject("manufacture"))).getString("address");
		
		assertEquals(appliedAddress, addressValue);
	}
	
	@SuppressWarnings("unchecked")
	public void testCheckMappingFunction() {
		String upcKey = "upc";
		String idKey = "id";
		String manufactureKey = "manufacture";
		String addressKey = "address";
		String upcValue = "UPCVALUE";
		String idValue = "idValue";
		
		TupleEntry teOutter = new TupleEntry(new Fields(upcKey, idKey, manufactureKey), Tuple.size(3));
		
		
		TupleEntry teMan = new TupleEntry(new Fields(addressKey, idKey), Tuple.size(2));
		
		String appliedAddress = "1234 Fake St.";
		teMan.setString(new Fields(addressKey), appliedAddress);
		
		teOutter.setString(upcKey, upcValue);
		teOutter.setString(idKey, idValue);
		teOutter.setObject(manufactureKey, teMan);
		
		Map<String, Object> m = InteropTools.createMap(teOutter);
		assertEquals(upcValue, m.get(upcKey));
		
		assertEquals(appliedAddress, ((Map<String, Object>)m.get(manufactureKey)).get(addressKey));
	}
	
	@SuppressWarnings("rawtypes")
	public void testComplexListConversion() {
		final String nameKey = "name";
		final String partsKey = "parts";
		
		final TupleEntry outter = new TupleEntry(new Fields(nameKey, partsKey), Tuple.size(2));
		
		final List<TupleEntry> parts = new ArrayList<TupleEntry>(2);
		for(int i = 0; i < 2; i++) {
			TupleEntry part = new TupleEntry(new Fields(nameKey, "number"), Tuple.size(2));
			part.setString(nameKey, "Part" + i);
			part.setInteger("number", i);
			parts.add(part);
		}
		
		outter.setString(nameKey, "GM");
		outter.setObject(partsKey, parts);
		
		Map<String, Object> map = InteropTools.createMap(outter);
		
		assertEquals("Part1", ((Map) ((List) map.get("parts")).get(1)).get(nameKey));
	}
	
	public void testSimpleListConversion() {
		final String nameKey = "parts";
		final List<String> parts = new ArrayList<String>();
		parts.add("Part1");
		parts.add("Part2");
		
		TupleEntry te = new TupleEntry(new Fields(nameKey), Tuple.size(1));
		te.setObject(nameKey, parts);
		
		Map<String, Object> map = InteropTools.createMap(te);
		assertEquals(parts, map.get(nameKey));
	}

}
