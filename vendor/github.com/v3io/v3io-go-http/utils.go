package v3io

import (
	"github.com/valyala/fasthttp"
)

func allocateResponse() *Response {
	return &Response{
		response: fasthttp.AcquireResponse(),
	}
}
