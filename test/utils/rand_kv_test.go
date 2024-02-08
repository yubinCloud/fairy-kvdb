package utils

import (
	"fairy-kvdb/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRandomTestKey(t *testing.T) {
	for i := 0; i < 10; i++ {
		key := utils.RandomTestKey(i)
		assert.NotNil(t, key)
	}
}

func TestRandomTestValue(t *testing.T) {
	for n := 0; n < 10; n++ {
		value := utils.RandomTestValue(n)
		assert.Equal(t, n, len(value))
	}
}
