module github.com/v3io/v3io-tsdb

go 1.14

require (
	github.com/cespare/xxhash v1.1.0
	github.com/cpuguy83/go-md2man v1.0.10 // indirect
	github.com/ghodss/yaml v1.0.0
	github.com/imdario/mergo v0.3.7
	github.com/nuclio/logger v0.0.1
	github.com/nuclio/nuclio-sdk-go v0.0.0-20190205170814-3b507fbd0324
	github.com/nuclio/zap v0.0.2
	github.com/pkg/errors v0.8.1
	github.com/rcrowley/go-metrics v0.0.0-20181016184325-3113b8401b8a
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/testify v1.4.0
	github.com/v3io/frames v0.7.33
	github.com/v3io/v3io-go v0.1.9
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
)

replace (
	github.com/v3io/v3io-tsdb => ./
	github.com/xwb1989/sqlparser => github.com/v3io/sqlparser v0.0.0-20190306105200-4d7273501871
	labix.org/v2/mgo => github.com/go-mgo/mgo v0.0.0-20180705113738-7446a0344b7872c067b3d6e1b7642571eafbae17
	launchpad.net/gocheck => github.com/go-check/check v0.0.0-20180628173108-788fd78401277ebd861206a03c884797c6ec5541
)
