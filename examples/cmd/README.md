[![Build Status](https://travis-ci.org/v3io/v3io-tsdb.svg?branch=master)](https://travis-ci.org/v3io/v3io-tsdb)

# V3IO-TSDB CLI usage examples

> Note: You could execute the V3IO-TSDB CLI from the directory where `tsdbctl` executable is located or by specifying the full path to the executable 
## To see all available options
 Execute `./tsdbctl -h` 
## To get help for specific command
Use the following syntax `tsdbctl <command> -h`<br>
For example to get help for `create` use:
```ecmascript 6
    ./tsdbctl create -h
```
* All CLI commands can accept the following global options
    ```ecmascript 6
    Global Flags:
      -c, --config string              path to yaml config file
      -p, --dbpath string              sub path for the TSDB, inside the container
      -s, --server string              V3IO Service URL - username:password@ip:port/container
      -v, --verbose string[="debug"]   Verbose output
    ```
## Create database instance
* 
    ```ecmascript 6
    Usage:
      tsdbctl create [flags]
    
    Flags:
      -t, --chunk-hours int       number of hours in a single chunk (default 1)
      -d, --days int              number of days covered per partition (default 1)
      -h, --help                  help for create
      -i, --rollup-interval int   aggregation interval in minutes (default 60)
      -r, --rollups string        Default aggregation rollups, comma seperated: count,avg,sum,min,max,stddev
    ```
* Create a database at `path-to-db` with some aggregations and `30 min` rollup interval 
    ```ecmascript 6
	 ./tsdbctl create -p <path-to-db> -r count,sum,max -i 30
    ```
* Create a database at `path-to-db` with some aggregations and `5 min` rollup interval and retention of `7 days`
    ```ecmascript 6
	 ./tsdbctl create -p <path-to-db> -r count,min,max,avg,stddev,last -i 5 -d 7
    ``
## Display metadata of V3IO-TSDB instance
* 
    ```ecmascript 6
    Usage:
      tsdbctl info [flags]
    
    Flags:
      -h, --help      help for info
      -m, --metrics   count number metric objects
      -n, --names     return metric names
    ```
* To display database metadata only
    ```ecmascript 6
    ./tsdbctl info
    ```     
* To display database info with metric names (types)  
    ```ecmascript 6
    ./tsdbctl info -n
	````
* To display database info with count of metric objects
    ```ecmascript 6
    ./tsdbctl info -m
	````
## Add samples to an existing database instance
* 
    ```ecmascript 6
      Usage:
        tsdbctl add <metric> [labels] [flags]
      
      Aliases:
        add, append
      
      Flags:
            --delay int       Add delay per insert batch in milisec
        -f, --file string     CSV input file
        -h, --help            help for add
        -t, --times string    time array, comma separated
        -d, --values string   values array, comma separated
    ```
* Append sample to **_cpu_** metric with labels **_os, node_** and set its value to **_73.2_** at the current time (_i.e. Now()_)
    ```ecmascript 6
	 ./tsdbctl add cpu os=win,node=xyz123 -d 73.2
	```
* Append given time series to **tsdb-example**
    ```ecmascript 6
	 ./tsdbctl add cpu os=win,node=xyz123 -t now-5m,now-4m,now-3m -d 73.2,45.1,33.33 -p tsdb-example
    # alternatively you can use timestamp in milliseconds
    ./tsdbctl add cpu os=win,node=xyz123 -t 1533026403000,1533026404000,1533026405000 -d 23.7,15.3,6.43 -p tsdb-example
	```
* Add series of samples from CSV
    ```ecmascript 6
    # assuming that TSDB instance "tsdb-intraday-example" has been created in advance
    ./tsdbctl add -f intraday.csv -p tsdb-intraday-example
    ```
    Example content (input CSV)
    ```ecmascript 6
    open,stock=AAL,41.460000,1529659800000
    high,stock=AAL,41.460000,1529659800000
    low,stock=AAL,41.440000,1529659800000
    close,stock=AAL,41.450000,1529659800000
    volume,stock=AAL,58739.000000,1529659800000
    ```
## Query the database
  * 
    ```
        Usage:
          tsdbctl query name [flags]
        
        Aliases:
          query, get
        
        Flags:
          -a, --aggregators string   comma separated list of aggregation functions, e.g. count,avg,sum,min,max,stddev,stdvar,last,rate
          -b, --begin string         from time
          -e, --end string           to time
          -f, --filter string        v3io query filter e.g. method=='get'
          -h, --help                 help for query
          -l, --last string          last min/hours/days e.g. 15m
          -o, --output string        output format: text,csv,json
          -i, --step string          interval step for aggregation functions
          -w, --windows string       comma separated list of overlapping windows
    ```
* Display all **cpu** metrics for Windows from the last hour; Output in CSV format.
	``` 
	 ./tsdbctl query cpu -f "os=='win'" -l 1h -o csv
    ```
* Display **stddev** aggregation for all metrics that match given filter criteria and time interval; Also use custom configuration.
    ```ecmascript 6
    ./tsdbctl query -l 6h -i 1h -a stddev -c ~/go/bin/v3io-custom.yaml -f "__name__=='Name_A_7' and  Label_A_1=='A_2'"
    ```
* Display **avg** and **count** for metric **Name_A_1** from the **tsdb-example** database for last **72 hours** 
    ```ecmascript 6
    ./tsdbctl query Name_A_1 -l 72h -i 12h -a avg,count -p tsdb-example
    ```
* Display **raw** data in **CSV** format for metrics that match given filter expression for certain period of time.
    ```ecmascript 6
    ./tsdbctl query -b now-13h -e now-12h -f "__name__=='Name_A_9' and Label_A_1=='A_1' and Label_A_2=='A_1'" -p tsdb-example -o csv
    ```
* Display **stddev** in **CSV** format for metric names that starts with **_Name_Z_1_** match given **filter expression** for certain period of time with **rollup interval** of **30 minutes**.
    ```ecmascript 6
    ./tsdbctl query -b now-13h -e now-12h -i 30m -a stddev -f "starts(__name__, 'Name_Z_1') and Label_A_1=='A_1' and Label_A_2=='A_1'" -p /tsdb-example -o csv
    ```
* Display **count** in **CSV** format for all metric names that starts with **_stock_sign_** for **last 7 days**
    ```ecmascript 6
    ./tsdbctl query -l 7d -i 7d -a count -f "starts(__name__, 'stock_sign')" -p tsdb-example -o csv
    ```
