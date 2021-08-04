package ratelimiter

import (
	"fmt"
	"github.com/vmware/transport-go/model"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// TODO
//  - tests
//  - middleware for rate limiter
var rateLimiterInstance RateLimitter

type RateLimitter interface {
	TestHttpRequest(req *http.Request, svcChannel string) (bool, error) // Test the incoming HTTP request against the rate limiter rules
	TestFabricConnectionID(req *model.Request, svcChannel string) (bool, error) // Test the incoming Fabric request against the rate limiter rules
	AddRateLimitRule(svcChannel string, maxRequests int64, threshold time.Duration) int // create a new rule for the given Fabric service channel with maxRequests and threshold and return the index of the new rule object in the slice
}

func NewRateLimitter() RateLimitter {
	return &rateLimiter{
		rateRulesManagerMap: map[string]*rateRulesManager{},
		ipMap: map[string]*timestampsManager{},
		brokerIDMap: map[string]*timestampsManager{},
	}
}

func GetRateLimiterInstance() RateLimitter {
	if rateLimiterInstance == nil {
		rateLimiterInstance = NewRateLimitter()
	}
	return rateLimiterInstance
}

func DestroyRateLimiter() {
	rateLimiterInstance = nil
}

type rateLimiter struct {
	rateRulesManagerMap map[string]*rateRulesManager // rate limiting threshold per service
	ipMap map[string]*timestampsManager              // map used to store and track requests by HTTP request
	brokerIDMap map[string]*timestampsManager        // map used to store and track requests by broker ID
	ticker time.Ticker                               // timer to periodically perform clean up operations
	mu sync.Mutex
}

func (rl *rateLimiter) TestHttpRequest(req *http.Request, svcChannel string) (bool, error) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	clientAddr := req.RemoteAddr
	// if the request came through a proxy, get the real client address
	if len(req.Header.Get("X-Forwarded-For")) > 0 {
		clientAddr = req.Header.Get("X-Forwarded-For")
	}
	// remove port from clientAddr
	u, _ := url.Parse("http://"+clientAddr)

	_, ok := rl.ipMap[u.Hostname()]
	if !ok {
		rl.ipMap[u.Hostname()] = &timestampsManager{
			channel:    svcChannel,
			timestamps: nil,
		}
	}

	lastVisitedMap := rl.ipMap[u.Hostname()]
	return rl.handleIncomingRequest(svcChannel, lastVisitedMap)
}

func (rl *rateLimiter) TestFabricConnectionID(req *model.Request, svcChannel string) (bool, error) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	brokerId := req.BrokerDestination.ConnectionId
	_, ok := rl.brokerIDMap[brokerId]
	if !ok {
		rl.brokerIDMap[brokerId] = &timestampsManager{
			channel:    svcChannel,
			timestamps: nil,
		}
	}

	lastVisitedMap := rl.brokerIDMap[brokerId]
	return rl.handleIncomingRequest(svcChannel, lastVisitedMap)
}

func (rl *rateLimiter) AddRateLimitRule(svcChannel string, maxRequests int64, rate time.Duration) int {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rulesMgr, exists := rl.rateRulesManagerMap[svcChannel]
	if !exists {
		rl.rateRulesManagerMap[svcChannel] = &rateRulesManager{
			channel: svcChannel,
			rules:   make([]*rule, 0),
		}
		rulesMgr = rl.rateRulesManagerMap[svcChannel]
	}
	return rulesMgr.addRule(rate, maxRequests)
}

func (rl *rateLimiter) handleIncomingRequest(svcChannel string, mgr *timestampsManager) (bool, error) {
	// delete old timestamps older than a day. (could be moved out of this function and be put on a schedule)
	mgr.deleteTimestampOlderThan(24 * time.Hour)

	// add a new timestamp
	now := time.Now()
	mgr.addTimestamp(&now)

	// iterate through each rule by counting the number of timestamps that fall within the
	// time window as defined by the rule, and if the count is greater than the maximum requests specified in the rule
	// return false with an error message indicating the limit has been reached
	for _, ru := range rl.rateRulesManagerMap[svcChannel].rules {
		var matchingCounts int64 = 0

		curr := mgr.timestamps
		for curr != nil {
			diff := -time.Until(*curr.time)
			if diff < ru.duration {
				matchingCounts++
			}
			curr = curr.next
		}

		// if any of the rules is violated, terminate early by returning false
		if matchingCounts > ru.maxRequests {
			return false, fmt.Errorf("maximum rate reached: %d out of %d", matchingCounts, ru.maxRequests)
		}
	}

	return true, nil
}
