# Running the benchmark test

## Prerequisites
* A fully operational V3IO Setup (Data and Application nodes)
 
## Configuration

### Create custom `v3io.yaml` configuration. Use the following example for reference:
```
    # File: v3io-custom.yaml
    
    # NGINX server address:PORT
    v3ioUrl: "localhost:8081"
    
    # V3IO container ID or Name
    container: "bigdata"
    
    # Path in the container
    path: "igor"
    
    # Logging level. Valid values: debug,info,warn,error (Default: info)
    verbose: "warn"
``` 
### Create TSDB instance using `tsdbctl`. Use the following shell script for reference:
```
    #!/bin/bash
    
    if [ "$1" == "" ]; then
      TSDB_PATH="pmetric"
    else
      TSDB_PATH=$1
    fi
    
    echo "Creating TSDB instance at $TSDB_PATH"
    
    # Create TSDB instance "tsdb" with some pre-aggregations and interval of 5 seconds (-v for verbose mode)
    SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
    $SCRIPTPATH/tsdbctl create -p $TSDB_PATH -r avg,count,sum,max -i 5 -v -c $SCRIPTPATH/v3io-custom.yaml
    
    echo Done.
```
Note, the above script assumes that all configuration and executable files reside at the `${HOME}/go/bin` directory.
### Define following environment variables. Optional `(you can do it in script)`
    * V3IO_URL="<Application Node Address>:8081" 
    * V3IO_TSDBCFG_PATH="$HOME/go/bin/v3io-custom.yaml"
### Run RundomIngest benchmark test for desired time period to populate the TSDB. Use the following script as a reference:
```
    #!/bin/bash
    
    #File: ingest.sh
    
    if [ "$1" == "" ]; then
      BENCH_TIME="1m"
    else
      BENCH_TIME="$1"
    fi
    
    echo "Ingesting random samples (Bench Time: $BENCH_TIME) ..."
    
    cd $HOME/go/src/github.com/v3io/v3io-tsdb/cmd/tsdbctl
    time V3IO_URL="localhost:8081" V3IO_TSDBCFG_PATH="$HOME/go/bin/v3io-custom.yaml" go test -benchtime $BENCH_TIME -run=DO_NOT_RUN_TESTS -bench=RandomIngest ../../nuclio/ingest
    
    echo Done
```
### Run Query using `tsdbctl` or use the following shell script
```
    #!/bin/bash
    
    # File: query.sh
    
    GOBIN=$HOME/go/bin
    TSDB_ROLLUP_INTERVAL=5m
    
    for ((i =0; i < 20; i++))
    do
      $GOBIN/tsdbctl query cpu_$i -a count -l 23h -i $TSDB_ROLLUP_INTERVAL  -c $GOBIN/v3io-custom.yaml
    done
    
    for ((i =0; i < 10; i++))
    do
      $GOBIN/tsdbctl query eth_$i -a count -l 23h -i $TSDB_ROLLUP_INTERVAL  -c $GOBIN/v3io-custom.yaml
    done
    
    $GOBIN/tsdbctl query mem -a count -l 23h -i $TSDB_ROLLUP_INTERVAL  -c $GOBIN/v3io-custom.yaml
    
    echo Done
```
### Example: Calculate total count
```
    #!/bin/bash
   
    # Flile: count-all.sh
     
    echo Fetching...
    
    GOBIN=$HOME/go/bin
    COUNT="$($GOBIN/query.sh | grep -v "v=0" | grep 2018 | cut -d'=' -f 2 | awk '{s+=$1} END {print s}')"
    
    echo Total samples count is: $COUNT
```