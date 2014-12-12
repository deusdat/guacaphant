package com.deusdatsolutions.guacaphant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cascading.tuple.TupleEntry;

public class InteropTools {
	
	/**
	 * 
	 * @param te a tupleentry that represents a json object. The names of the fields
	 * are mapped to the keys in the json. The values of the fields are mapped to either as
	 * straight json types, or if the value is a te, into another map.
	 * @return
	 */
	public static Map<String, Object> createMap(TupleEntry te) {
		final Map<String, Object> map = new HashMap<String, Object>();
		
		@SuppressWarnings("unchecked")
		Iterator<String> iterator = te.getFields().iterator();
		while(iterator.hasNext()) {
			final String next = iterator.next();
			Object object = te.getObject(next);
			if(object instanceof TupleEntry) {
				object = createMap((TupleEntry) object);
			} else if(object instanceof Collection<?>) {
				Collection<?> entries = (Collection<?>) object;
				List<Object> mapped = new ArrayList<Object>(entries.size());
				for(Iterator<?> it = entries.iterator(); it.hasNext();) {
					Object rawNext = it.next();
					if(rawNext instanceof TupleEntry) {
						mapped.add(createMap((TupleEntry) rawNext));
					} else {
						mapped.add(rawNext);
					}
				}
				object = mapped;
			}
			map.put(next, object);
		}
		
		return map;
	}
}
