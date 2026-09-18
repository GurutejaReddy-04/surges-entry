package main

// DetermineAnomaly applies a two-tier rule to detect anomalous values.
//
// Tier 1 (Primary - Redis available): If rollingAvg is available, flag anomaly
// if value exceeds (rollingAvg * deviationMultiplier).
//
// Tier 2 (Fallback - Redis unavailable): If rollingAvg is nil, fallback to a static
// absolute threshold check (value > fallbackThreshold).
func DetermineAnomaly(value float64, rollingAvg *float64, deviationMultiplier, fallbackThreshold float64) bool {
	if rollingAvg != nil {
		threshold := *rollingAvg * deviationMultiplier
		return value > threshold
	}

	return value > fallbackThreshold
}
