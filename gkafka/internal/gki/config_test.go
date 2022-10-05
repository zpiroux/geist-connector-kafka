package gki

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoaderConfig(t *testing.T) {

	cfg := Config{
		configMap: make(ConfigMap),
	}

	inputConfigMap := ConfigMap{
		"bootstrap.servers":          "fooserver",
		"security.protocol":          "bar",
		"sasl.mechanism":             "baz",
		"sasl.username":              "usr",
		"sasl.password":              "pwd",
		"enable.idempotence":         true,
		"compression.type":           "zstd",
		"group.id":                   "mygrp",
		"session.timeout.ms":         298734,
		"max.poll.interval.ms":       2345,
		"enable.auto.commit":         true,
		"enable.auto.offset.store":   false,
		"queued.max.messages.kbytes": 23525,
	}

	expectedOutputConfigMap := ConfigMap{
		"bootstrap.servers":  "fooserver",
		"security.protocol":  "bar",
		"sasl.mechanism":     "baz",
		"sasl.username":      "usr",
		"sasl.password":      "pwd",
		"enable.idempotence": true,
		"compression.type":   "zstd",
	}

	cfg.SetLoaderProps(inputConfigMap)

	assert.Equal(t, expectedOutputConfigMap, cfg.configMap)
}
