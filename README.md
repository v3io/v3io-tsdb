# v3io-tsdb
iguazio API lib for time-series DB access and Prometheus TSDB storage driver 

see `v3io-tsdb_test.go` for example usage 
it works with the Prometheus fork found in `https://github.com/v3io/prometheus`

For use inside your code or nuclio function you just need to import this package (no need for Prometheus)

to work you need to use a configuration file in the minimal form:

```yaml
v3ioUrl: "<v3io address:port>"
container: "tsdb"
path: "metrics"
verbose: false
workers: 8
```
