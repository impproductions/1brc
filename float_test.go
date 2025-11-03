package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZeroToInt(t *testing.T) {
	result := parseInt([]byte("0.0"))
	expected := int32(0)
	assert.Equal(t, expected, result)
}

func TestIntToInt(t *testing.T) {
	result := parseInt([]byte("1.0"))
	expected := int32(10)
	assert.Equal(t, expected, result)
}

func TestLargeToInt(t *testing.T) {
	result := parseInt([]byte("1873.0"))
	expected := int32(18730)
	assert.Equal(t, expected, result)
}

func TestDecimalsToInt(t *testing.T) {
	result := parseInt([]byte("15.5"))
	expected := int32(155)
	assert.Equal(t, expected, result)
}

func TestNegativeToInt(t *testing.T) {
	result := parseInt([]byte("-15.5"))
	expected := int32(-155)
	assert.Equal(t, expected, result)
}

func TestTestAllDigits(t *testing.T) {
	result := parseInt([]byte("99.9"))
	expected := int32(999)
	assert.Equal(t, expected, result)
}
