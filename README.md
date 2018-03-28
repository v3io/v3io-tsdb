# V3IO-TSDB
iguazio API lib for time-series DB access and Prometheus TSDB storage driver 

## Overview
iguazio provides a real-time flexible document database engine which accelerates popular BigData frameworks such as 
Spark and Presto, as well as provide AWS compatible data APIs (DynamoDB, Kinesis, S3). iguazio DB engine runs at the
speed of in-memory databases, but uses lower cost and higher density Flash, leveraging a unique low-level design. 

iguazio DB low-level APIs (v3io) has rich API semantics and multiple indexing types, those allow it to run multiple
workloads and processing engines on exactly the same data, and consistently read/write the data in different tools.

This project uses v3io semantics (row & col layouts, arrays, random & sequential indexes, etc.) to provide extreamly
fast and scalable Time Series database engine which can be accessed simultaneously by multiple engines, such as:
- Prometheus TimeSeries DB (for metrics scraping & queries)
- nuclio serverless functions (for real-time ingestion, stream processing or queries) 
- iguazio DynamoDB API (with extensions) 
- Apache Presto & Spark (future item)

The solution stores the raw data in highly compressed columns (using Gorilla/XOR compression), and aggregates in Arrays.
High-resolution queries selectively access the array ranges which can significantly accelerate searches. 
Metric names and labels are stored in search optimized keys and string attributes.
 
iguazio DB engine can run full dimension scan (searches) in the rate of millions of metrics per second, or use selective
range based queries to access a specific metric family. The random access keys (Hash based) allow real-time
sample data ingestion and stream processing.      

## How To Use  

> Note: This project is still under development 

see `v3io-tsdb_test.go` for example usage as an embedded library 

it works with the Prometheus fork found in `https://github.com/v3io/prometheus`

For use inside your code or nuclio function you just need to import this package (no need for Prometheus)
, see function example under [\nuclio](nuclio)

to work you need to use a configuration file in the minimal form:

```yaml
v3ioUrl: "<v3io address:port>"
container: "tsdb"
path: "metrics"
verbose: false
workers: 8
```
