module github.com/v3io/v3io-tsdb

go 1.19

require (
	github.com/cespare/xxhash v1.1.0
	github.com/ghodss/yaml v1.0.0
	github.com/imdario/mergo v0.3.7
	github.com/nuclio/logger v0.0.1
	github.com/nuclio/nuclio-sdk-go v0.0.0-20190205170814-3b507fbd0324
	github.com/nuclio/zap v0.1.2
	github.com/pkg/errors v0.8.1
	github.com/rcrowley/go-metrics v0.0.0-20181016184325-3113b8401b8a
	github.com/spf13/cobra v0.0.3
	github.com/stretchr/testify v1.8.1
	github.com/v3io/frames v0.10.5
	github.com/v3io/v3io-go v0.3.0
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
)

require (
	github.com/andybalholm/brotli v1.0.4 // indirect
	github.com/cpuguy83/go-md2man v1.0.10 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.2.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/liranbg/uberzap v1.20.0-nuclio.1 // indirect
	github.com/logrusorgru/aurora/v3 v3.0.0 // indirect
	github.com/nuclio/errors v0.0.4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/russross/blackfriday v1.5.2 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.44.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/net v0.7.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	google.golang.org/genproto v0.0.0-20180817151627-c66870c02cf8 // indirect
	google.golang.org/grpc v1.20.0 // indirect
	gopkg.in/yaml.v2 v2.2.8 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	zombiezen.com/go/capnproto2 v2.17.0+incompatible // indirect
)

replace (
	github.com/v3io/v3io-go => github.com/v3io/v3io-go v0.3.0
	github.com/v3io/v3io-tsdb => ./
	github.com/xwb1989/sqlparser => github.com/v3io/sqlparser v0.0.0-20190306105200-4d7273501871
	labix.org/v2/mgo => github.com/go-mgo/mgo v0.0.0-20180705113738-7446a0344b7872c067b3d6e1b7642571eafbae17
	launchpad.net/gocheck => github.com/go-check/check v0.0.0-20180628173108-788fd78401277ebd861206a03c884797c6ec5541
)
