package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitation(tst *testing.T) {
	config := &V3ioConfig{
		AccessKey: "12345",
		Username:  "moses",
		Password:  "bla-bla-password",
	}

	configAsString := config.String()

	// Name should not be sanitized
	assert.Contains(tst, configAsString, "moses")

	// sensitive fields must be sanitized
	assert.NotContains(tst, configAsString, "12345")
	assert.NotContains(tst, configAsString, "bla-bla-password")

	// original object should not be changed
	assert.Equal(tst, config.AccessKey, "12345")
	assert.Equal(tst, config.Password, "bla-bla-password")
}
