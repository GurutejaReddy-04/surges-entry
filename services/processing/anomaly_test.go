package main

import "testing"

func TestDetermineAnomaly_Tier1_WithRollingAvg(t *testing.T) {
	avg := 100.0
	multiplier := 1.5
	fallback := 1000.0

	// Threshold is 100.0 * 1.5 = 150.0
	tests := []struct {
		name        string
		value       float64
		wantAnomaly bool
	}{
		{
			name:        "normal value well within threshold",
			value:       110.0,
			wantAnomaly: false,
		},
		{
			name:        "value exactly at threshold",
			value:       150.0,
			wantAnomaly: false,
		},
		{
			name:        "value exceeding threshold",
			value:       150.01,
			wantAnomaly: true,
		},
		{
			name:        "massive outlier",
			value:       500.0,
			wantAnomaly: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetermineAnomaly(tt.value, &avg, multiplier, fallback)
			if got != tt.wantAnomaly {
				t.Errorf("DetermineAnomaly() = %v, want %v for value %f", got, tt.wantAnomaly, tt.value)
			}
		})
	}
}

func TestDetermineAnomaly_Tier2_FallbackWithoutRollingAvg(t *testing.T) {
	multiplier := 1.5
	fallback := 1000.0

	tests := []struct {
		name        string
		value       float64
		wantAnomaly bool
	}{
		{
			name:        "normal value under fallback threshold",
			value:       150.0,
			wantAnomaly: false,
		},
		{
			name:        "value exactly at fallback threshold",
			value:       1000.0,
			wantAnomaly: false,
		},
		{
			name:        "value above fallback threshold",
			value:       1000.5,
			wantAnomaly: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetermineAnomaly(tt.value, nil, multiplier, fallback)
			if got != tt.wantAnomaly {
				t.Errorf("DetermineAnomaly() = %v, want %v for fallback value %f", got, tt.wantAnomaly, tt.value)
			}
		})
	}
}
