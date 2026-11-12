package main

import (
	"reflect"
	"testing"
)

func TestParseBrokers(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{
			name: "single broker",
			raw:  "localhost:9092",
			want: []string{"localhost:9092"},
		},
		{
			name: "comma separated with spaces",
			raw:  "broker1:9092, , broker2:9092 ",
			want: []string{"broker1:9092", "broker2:9092"},
		},
		{
			name: "empty fallback",
			raw:  " , ",
			want: []string{"localhost:9092"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseBrokers(tc.raw)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("parseBrokers(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestLoadConfig_BrokersAndValidation(t *testing.T) {
	t.Setenv("KAFKA_BROKER", "")
	t.Setenv("KAFKA_BROKERS", "k1:9092,k2:9092")
	t.Setenv("EVENT_RATE", "0")
	t.Setenv("NUM_USERS", "-5")

	cfg := loadConfig()
	if cfg.KafkaBroker != "k1:9092,k2:9092" {
		t.Fatalf("expected KAFKA_BROKERS fallback, got %q", cfg.KafkaBroker)
	}
	if cfg.EventRate != 5 {
		t.Fatalf("expected EventRate fallback 5 for non-positive value, got %d", cfg.EventRate)
	}
	if cfg.NumUsers != 10 {
		t.Fatalf("expected NumUsers fallback 10 for non-positive value, got %d", cfg.NumUsers)
	}
}
