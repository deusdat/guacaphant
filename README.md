# guacaphant

A tap to integrate Hadoop and ArangoDB via Cascading. Leverage AQL to find the documents you want to pull into Hadoop for further analysis. 

The current version only supports sourcing ArangoDB. I'm working on sinking for the next iteration.

## From Documents to Tuples
Cascading deals with Tuples. TupleEntry's are a nice wrapper around tuples if the column names are known ahead of time.

ArangoDB deals with documents. Really simple documents pretty much map to the Tuple/TupleEntry paradigm. Nested documents are converted into TupleEntry. So say you have a person document with **"name"**, **"age"** and **"address"**. **"address"** is an  object with fields **"street"**,** "city" **and **"state"**. Guacaphant will create a root TupleEntry with the Fields** "address"**, **"age" **and **"name"**.  You can access **"age"** and **"name"** as strings. **"address"** is a TupleEntry with the matching field names.

## Scheme Fields
Guacaphant has two scheme constructors: with Fields and without Fields. If you specify fields, then the scheme will only load columns that match. If there is an attribute in the document that's not in the Fields list, an cascading.tuple.FieldResolverException will occur. A good practice is to have your Fields match your return clause. For example:

```java
final String returnClause = "RETURN {\"name\": u.name, \"age\" : u.age, \"other\" : u.other}"
final Fields inputFields = new Fields("name", "age", "other");
```

This will only return the three attributes specified. If you want to include the _id, or other document related meta-data, just manually include it in your return clause and Fields defintion.

Using the constructor without the Fields value will set the value to Fields.UNKNOWN. At this point you have to manually manipulate the piping because Cascading can't tell what's going on inside. This option is there for convience.

It's best practice to use the specific columns approach.

## Example Sourcing ArangoDB
```java
package guac;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import com.deusdatsolutions.guacaphant.ArangoDBScheme;
import com.deusdatsolutions.guacaphant.ArangoDBTap;

public class Main {
    public static void main(String[] args) {
    Properties props = new Properties();
    AppProps.setApplicationJarClass(props, Main.class);

    ArangoDBScheme scheme = new ArangoDBScheme("Testing", "FOR u IN Users",
        "RETURN u", "u.name DESC", 2, new Fields("name", "age");

    ArangoDBTap input = new ArangoDBTap(scheme, "arangodb");

    Scheme outScheme = new TextDelimited(Fields.ALL, ",");
    Hfs devNull = new Hfs(outScheme, "/tmp/righthere", SinkMode.REPLACE);

    Pipe in = new Pipe("FromArango");
    Pipe sort = new GroupBy(in, new Fields("name"));
    Pipe out = new Each(sort, new Identity());

    FlowDef flowDef = new FlowDef().addSource(in, input).addTailSink(out,
        devNull);

    Flow flow = new HadoopFlowConnector().connect(flowDef);
    flow.start();
    }
}
```
