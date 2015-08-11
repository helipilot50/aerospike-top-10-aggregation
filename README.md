# Finding the Top 10 using Aerospike Aggregations

##Problem
You want to create a leaderboard of the top 10 scores, or 10 most recent events, using Aerospike as the data store

##Solution
The solution is to use an Aggregation that processes the stream of tuples flowing from a query on a secondary index. The aggregation is done in code on each node in the cluster and finally aggregated, or reduced, in the client.

### How to build

The source code for this solution is available on GitHub,  
https://github.com/helipilot50/aerospike-top-10-aggregation.git. 


This example requires a working Java development environment (Java 6 and above) including Maven (Maven 2). The Aerospike Java client will be downloaded from Maven Central as part of the build.

After cloning the repository, use maven to build the jar files. From the root directory of the project, issue the following command:
```bash
mvn clean package
```
A JAR file will be produced in the directory 'target', `aerospike-top-10-1.0-full.jar`

###Running the solution
This is a runnable jar complete with all the dependencies packaged.

To load data use this command:
```bash
java -jar aerospike-top-10-1.0-full.jar -l
```
It will generate 100,000 `Event` records with an event name and a time stamp.

To verify you have loaded data use this command:
```bash
java -jar aerospike-top-10-1.0-full.jar -S
```

You can run the aggregation with the following command:
```bash
java -jar aerospike-top-10-1.0-full.jar -q
```
This program will load a User Defined Function (UDF) module when it starts. It will look for the UDF module at this location `udf/leaderboard.lua`. Be sure you place it there.

####Options
```
-a,--all              Aggregate all using ScanAggregate.
-h,--host <arg>       Server hostname (default: 127.0.0.1)
-l,--load             Load data.
-n,--namespace <arg>  Namespace (default: test)
-p,--port <arg>       Server port (default: 3000)
-q,--query            Aggregate with query.
-s,--set <arg>        Set (default: demo)
-S,--scan			  Scan all for testing.
-u,--usage            Print usage.
```

#### Output
The output is a List of 10 Maps, in highest to lowest order:
```
{eventid=Event:100000, time=1421955197267}
{eventid=Event:99999, time=1421955197266}
{eventid=Event:99998, time=1421955197265}
{eventid=Event:99996, time=1421955197259}
{eventid=Event:99997, time=1421955197259}
{eventid=Event:99995, time=1421955197259}
{eventid=Event:99994, time=1421955197258}
{eventid=Event:99993, time=1421955197258}
{eventid=Event:99992, time=1421955197256}
{eventid=Event:99991, time=1421955197255} 
```


##Discussion

The Java code is very simple, in the `main()` method a secondary index is created on the `time` Bin and the UDF module is registered with the cluster.
```java
/*
 * Create index for query
 * Index creation only needs to be done once and can be done using AQL or ASCLI also
 */
IndexTask it = as.client.createIndex(null, as.namespace, as.set, "top-10", TIME_BIN, IndexType.NUMERIC);
it.waitTillComplete();
/*
 * Register UDF module
 * Registration only needs to be done after a change in the UDF module.
 */
RegisterTask rt = as.client.register(null, "udf/leaderboard.lua", "leaderboard.lua", Language.LUA);
rt.waitTillComplete();
```
Based on the option from the command line the code will either load data or run the aggregation.

```java

if (cl.hasOption("l")) {
	as.populateData();
	return;
} else if (cl.hasOption("q")) {
	as.queryAggregate();
	return;
} else if (cl.hasOption("a")) {
	as.scanAggregate();
	return;
} else {
	logUsage(options);
}
```
You will note option `-a`; this performs an aggregation by scanning the whole set rather than by using a secondary index and it is only supported on the latest version of Aerospike.

The `queryAggregate()` method creates the `Statement` for the query and then calls the `aggregate()` method, which uses the Aerospike `queryAggregate()` operation to query the data and invoke the StreamUDF `top()` in the module `leaderboard`.

```java
	public void queryAggregate() {
		long now = System.currentTimeMillis();
		long yesterday = now - 24 * 60 * 60 * 1000;
		Statement stmt = new Statement();
		stmt.setNamespace(this.namespace);
		stmt.setSetName(this.set);
		stmt.setBinNames(EVENT_ID_BIN, TIME_BIN);
		stmt.setFilters(Filter.range(TIME_BIN, yesterday, now));
		aggregate(stmt);
	}
	
	private void aggregate(Statement stmt){
		ResultSet rs = this.client.queryAggregate(null, stmt, 
					"leaderboard", "top", Value.get(10));
		
		while (rs.next()){
			List<Map<String, Object>> result =  
					(List<Map<String, Object>>) rs.getObject();
			for (Map<String, Object> element : result){
				System.out.println(element);
			}
		}
	}
```
The heavy lifting is done in the Stream UDF to build a List of the top 10 (latest) events as they come from the query stream.

The stream is processed with a `Map()` function, then an `Aggregate()` function and finally a `Reduce()` function.

Let's look at each one, then string them together to process the stream.

#### Map()

The purpose of a `map()` function is to transform the current element in the stream to a new form. In this example we are transforming a `Record` to a `Map`.

While it looks almost the same, the `transformer()` function is discarding the meta data associated with the `Record`, and retaining only the information we are interested in.
```lua
  local function transformer(rec)
    --info("rec:"..tostring(rec))
    local touple = map()
    touple["eventid"] = rec["eventid"]
    touple["time"] = rec["time"]
    --info("touple:"..tostring(touple))
    return touple
  end
```
The `map()` function is invoked on each node in the cluster for *every* element in the stream.

#### Aggregate()
The purpose of the `aggregate()` function is to accumulate a result from the elements in the stream. In this example, the `accumulate()` uses a `List` of 10 elements, as a new element arrives it is inserted into the list in the correct order. The local function `movedown()` aids in this. 
```lua
   local function movedown(theList, size, at, element)
    --info("List:"..tostring(theList)..":"..tostring(size)..":"..tostring(start))
    if at > size then
      info("You are an idiot")
      return
    end 
    index = size-1
    while (index > at) do
      theList[index+1] = theList[index]
      index = index -1
    end
    
    theList[at] = element
  end


 local function accumulate(aggregate, nextitem)
    local aggregate_size = list.size(aggregate)
      --info("Item:"..tostring(nextitem))
      index = 1
      for value in  list.iterator(aggregate) do
        --info(tostring(nextitem.time).." > "..tostring(value.time))
        if nextitem.time > value.time then
          movedown(aggregate, top_size, index, nextitem)
          break
        end
        index = index + 1
      end
    return aggregate
  end
```
The `aggregate()` function is invoked for every element in the stream on each node in the cluster. **Note:** The `aggregate` variable is held in RAM, so watch for high memory usage for large elements.

#### Reduce()
The `reduce()` function combines all of the results from the stream in to one complete result. It will be invoked on each node in the cluster and a final reduce on the client.

The function `reducer()` simply combines two elements -- in this case two ordered `Lists` that are the output of two `Aggregation()` functions. The code uses a simple technique to take the two ordered Lists and return a new ordered list of the top 10.

```lua
  local function reducer( this, that )
    local merged_list = list()
    local this_index = 1
    local that_index = 1
    while this_index <= 10 do
      while that_index <= 10 do
        if this[this_index].time >= that[that_index].time then
          list.append(merged_list, this[this_index])
          this_index = this_index + 1
        else
          list.append(merged_list, that[that_index])
          that_index = that_index +1
        end
        if list.size(merged_list) == 10 then
          break
        end
      end
      if list.size(merged_list) == 10 then
        break
      end
    end
    --info("This:"..tostring(this).." that:"..tostring(that))
    return merged_list
  end
```

#### The stream function: top()

The stream function `top()` is the UDF called by the client. It takes a stream object as a parameter and configures a `map()` function, an `aggregate()` function and a `reduce()` function.

The functions that we have written to implement these stereotypes are passed in as function pointers.

*NOTE:* The `aggregate()` function also takes an additional parameter `list{}`. This in an initial `List` to be populated by the `aggregate()` function.

```lua
function top(flow, top_size)

	. . .
	  
  return flow:map(transformer):aggregate(
          list{
              map{eventid="ten",time=0},
              map{eventid="nine",time=0},
              map{eventid="eight",time=0},
              map{eventid="seven",time=0},
              map{eventid="six",time=0},
              map{eventid="five",time=0},
              map{eventid="four",time=0},
              map{eventid="three",time=0},
              map{eventid="two",time=0},
              map{eventid="one",time=0}
          }, accumulate):reduce(reducer)
end
```