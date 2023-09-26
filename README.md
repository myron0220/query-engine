# query-engine
This project demonstrates how to build a query engine, which is 'a piece of software that can execute queries against data to produce answers to questions'.  
The project is categraized into two folders: jvm (currently for the actual query engine), TPC-H V3.0.1 (for the test benchmark).  

## jvm
### benchmark specification
This project gets illustrations and is built upon another open source project, see `https://howqueryengineswork.com/01-what-is-a-query-engine.html` for more information.  
### how to start
`cd jvm`  
`gradlew tasks`  
`gradlew run [tasks you want to run]`

## TPC-H V3.0.1
### benchmark specification
This project uses TPC BENCHMARK H as the test benchmark.  
See `specification.pdf` for more information.
### how to start
See `https://gist.github.com/yunpengn/6220ffc1b69cee5c861d93754e759d08` for how to use the benchmark.

