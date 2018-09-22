package v3io

import (
	"fmt"

	"errors"
)

// ErrorWithStatusCode is an error that holds a status code
type ErrorWithStatusCode struct {
	error
	statusCode int
	message    string
}

// NewErrorWithStatusCode creates an error that holds a status code
func NewErrorWithStatusCode(statusCode int, format string, args ...interface{}) ErrorWithStatusCode {
	return ErrorWithStatusCode{
		error:      errors.New(fmt.Sprintf(format, args...)),
		statusCode: statusCode,
	}
}

// StatusCode returns the status code of the error
func (e *ErrorWithStatusCode) StatusCode() int {
	return e.statusCode
}
