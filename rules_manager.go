package ratelimiter

import "time"

// NOTE: rate rules are usually configured once and left idle.. so i'm not implementing a lock here and see how it goes.
type rateRulesManager struct {
	channel string
	rules []*rule
}

type rule struct {
	maxRequests int64
	duration time.Duration
}

func (rR *rateRulesManager) addRule(duration time.Duration, numOfRequests int64) int {
	rR.rules = append(rR.rules, &rule{
		maxRequests: numOfRequests,
		duration:    duration,
	})
	// return the index of the new entry so it can be used to find and delete from the
	return len(rR.rules) - 1
}

func (rR *rateRulesManager) deleteRule(idx int) {
	rR.rules = append(rR.rules[:idx], rR.rules[idx+1:]...)
}
