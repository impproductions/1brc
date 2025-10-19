package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZeroToFloat(t *testing.T) {
	result := parseFloat([]byte("0.0"))
	expected := float64(0.0)
	assert.Equal(t, expected, result)
}

func TestIntToFloat(t *testing.T) {
	result := parseFloat([]byte("1.0"))
	expected := float64(1.0)
	assert.Equal(t, expected, result)
}

func TestLargeToFloat(t *testing.T) {
	result := parseFloat([]byte("1873.0"))
	expected := float64(1873.0)
	assert.Equal(t, expected, result)
}

func TestDecimalsToFloat(t *testing.T) {
	result := parseFloat([]byte("15.5"))
	expected := float64(15.5)
	assert.Equal(t, expected, result)
}

func TestNegativeToFloat(t *testing.T) {
	result := parseFloat([]byte("-15.5"))
	expected := float64(-15.5)
	assert.Equal(t, expected, result)
}

func TestTestAllDigits(t *testing.T) {
	result := parseFloat([]byte("1234567890.0"))
	expected := float64(1234567890.0)
	assert.Equal(t, expected, result)
}
