// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"fmt"

	"github.com/nats-io/nats-streaming-server/util"
)

// AddPerChannel stores limits for the given channel `name` in the StoreLimits.
// Inheritance (that is, specifying 0 for a limit means that the global limit
// should be used) is not applied in this call. This is done in StoreLimits.Build
// along with some validation.
func (sl *StoreLimits) AddPerChannel(name string, cl *ChannelLimits) {
	if sl.PerChannel == nil {
		sl.PerChannel = make(map[string]*ChannelLimits)
	}
	sl.PerChannel[name] = cl
}

// Build sets the global limits into per-channel limits that are set
// to zero. This call also validates the limits. An error is returned if:
// * any global limit is set to a negative value.
// * the number of per-channel is higher than StoreLimits.MaxChannels.
// * a per-channel name is invalid
func (sl *StoreLimits) Build() error {
	// Check that there is no negative value
	if err := sl.checkGlobalLimits(); err != nil {
		return err
	}
	// If there is no per-channel, we are done.
	if len(sl.PerChannel) == 0 {
		return nil
	}
	if sl.MaxChannels > 0 && len(sl.PerChannel) > sl.MaxChannels {
		return fmt.Errorf("too many channels defined (%v). The max channels limit is set to %v",
			len(sl.PerChannel), sl.MaxChannels)
	}
	for cn := range sl.PerChannel {
		if !util.IsSubjectValid(cn) {
			return fmt.Errorf("invalid channel name %q", cn)
		}
	}

	// If we are here, it means that there was no error,
	// so we now apply inheritance.
	for _, cl := range sl.PerChannel {
		if cl.MaxSubscriptions < 0 {
			cl.MaxSubscriptions = 0
		} else if cl.MaxSubscriptions == 0 {
			cl.MaxSubscriptions = sl.MaxSubscriptions
		}
		if cl.MaxMsgs < 0 {
			cl.MaxMsgs = 0
		} else if cl.MaxMsgs == 0 {
			cl.MaxMsgs = sl.MaxMsgs
		}
		if cl.MaxBytes < 0 {
			cl.MaxBytes = 0
		} else if cl.MaxBytes == 0 {
			cl.MaxBytes = sl.MaxBytes
		}
		if cl.MaxAge < 0 {
			cl.MaxAge = 0
		} else if cl.MaxAge == 0 {
			cl.MaxAge = sl.MaxAge
		}
	}
	return nil
}

func (sl *StoreLimits) checkGlobalLimits() error {
	if sl.MaxChannels < 0 {
		return fmt.Errorf("max channels limit cannot be negative (%v)", sl.MaxChannels)
	}
	if sl.MaxSubscriptions < 0 {
		return fmt.Errorf("max subscriptions limit cannot be negative (%v)", sl.MaxSubscriptions)
	}
	if sl.MaxMsgs < 0 {
		return fmt.Errorf("max messages limit cannot be negative (%v)", sl.MaxMsgs)
	}
	if sl.MaxBytes < 0 {
		return fmt.Errorf("max bytes limit cannot be negative (%v)", sl.MaxBytes)
	}
	if sl.MaxAge < 0 {
		return fmt.Errorf("max age limit cannot be negative (%v)", sl.MaxAge)
	}
	return nil
}
