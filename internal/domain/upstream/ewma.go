package upstream

// EWMA is an exponentially weighted moving average. The first sample seeds
// the average so a new upstream is not compared against zero.
type EWMA struct {
	alpha  float64
	value  float64
	seeded bool
}

// NewEWMA returns an average where each sample has weight alpha in (0, 1].
func NewEWMA(alpha float64) EWMA {
	if alpha <= 0 || alpha > 1 {
		alpha = 0.3
	}
	return EWMA{alpha: alpha}
}

// Observe adds a sample.
func (e *EWMA) Observe(sample float64) {
	if !e.seeded {
		e.value, e.seeded = sample, true
		return
	}
	e.value = e.alpha*sample + (1-e.alpha)*e.value
}

// Value returns the average, or 0 before the first sample.
func (e *EWMA) Value() float64 { return e.value }

// Seeded reports whether at least one sample was observed.
func (e *EWMA) Seeded() bool { return e.seeded }
