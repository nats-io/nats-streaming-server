// Copyright 2016-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stores

import (
	"fmt"
	"time"

	"github.com/nats-io/nats-streaming-server/util"
)

// Used for display of limits
const (
	limitCount = iota
	limitBytes
	limitDuration
)

// Clone returns a copy of the store limits
func (sl *StoreLimits) Clone() *StoreLimits {
	cloned := *sl
	cloned.PerChannel = sl.ClonePerChannelMap()
	return &cloned
}

// ClonePerChannelMap returns a deep copy of the StoreLimits's PerChannel map
func (sl *StoreLimits) ClonePerChannelMap() map[string]*ChannelLimits {
	if sl.PerChannel == nil {
		return nil
	}
	clone := make(map[string]*ChannelLimits, len(sl.PerChannel))
	for k, v := range sl.PerChannel {
		copyVal := *v
		clone[k] = &copyVal
	}
	return clone
}

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

type channelLimitInfo struct {
	name        string
	limits      *ChannelLimits
	isLiteral   bool
	isProcessed bool
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
	literals := 0
	sublist := util.NewSublist()
	for cn, cl := range sl.PerChannel {
		if !util.IsChannelNameValid(cn, true) {
			return fmt.Errorf("invalid channel name %q", cn)
		}
		isLiteral := util.IsChannelNameLiteral(cn)
		if isLiteral {
			literals++
			if sl.MaxChannels > 0 && literals > sl.MaxChannels {
				return fmt.Errorf("too many channels defined (%v). The max channels limit is set to %v",
					literals, sl.MaxChannels)
			}
		}
		cli := &channelLimitInfo{
			name:      cn,
			limits:    cl,
			isLiteral: isLiteral,
		}
		sublist.Insert(cn, cli)
	}
	// If we are here, it means that there was no error,
	// so we now apply inheritance.
	sl.applyInheritance(sublist)
	return nil
}

func (sl *StoreLimits) applyInheritance(sublist *util.Sublist) {
	// Get the subjects from the sublist. This ensure that they are ordered
	// from the widest to the narrowest of subjects.
	channels := sublist.Subjects()
	for _, cn := range channels {
		r := sublist.Match(cn)
		// There has to be at least 1 match (the current channel name we
		// are trying to match).
		channel := r[0].(*channelLimitInfo)
		if channel.isLiteral && channel.isProcessed {
			continue
		}
		if !channel.isProcessed {
			sl.inheritLimits(channel, &sl.ChannelLimits)
		}
		prev := channel
		for i := 1; i < len(r); i++ {
			channel = r[i].(*channelLimitInfo)
			if !channel.isProcessed {
				sl.inheritLimits(channel, prev.limits)
			}
			prev = channel
		}
	}
}

func (sl *StoreLimits) inheritLimits(channel *channelLimitInfo, parentLimits *ChannelLimits) {
	cl := channel.limits
	if cl.MaxSubscriptions < 0 {
		cl.MaxSubscriptions = 0
	} else if cl.MaxSubscriptions == 0 {
		cl.MaxSubscriptions = parentLimits.MaxSubscriptions
	}
	if cl.MaxMsgs < 0 {
		cl.MaxMsgs = 0
	} else if cl.MaxMsgs == 0 {
		cl.MaxMsgs = parentLimits.MaxMsgs
	}
	if cl.MaxBytes < 0 {
		cl.MaxBytes = 0
	} else if cl.MaxBytes == 0 {
		cl.MaxBytes = parentLimits.MaxBytes
	}
	if cl.MaxAge < 0 {
		cl.MaxAge = 0
	} else if cl.MaxAge == 0 {
		cl.MaxAge = parentLimits.MaxAge
	}
	if cl.MaxInactivity < 0 {
		cl.MaxInactivity = 0
	} else if cl.MaxInactivity == 0 {
		cl.MaxInactivity = parentLimits.MaxInactivity
	}
	channel.isProcessed = true
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
	if sl.MaxInactivity < 0 {
		return fmt.Errorf("max inactivity limit cannot be negative (%v)", sl.MaxInactivity)
	}
	return nil
}

// Print returns an array of strings suitable for printing the store limits.
func (sl *StoreLimits) Print() []string {
	sublist := util.NewSublist()
	for cn, cl := range sl.PerChannel {
		sublist.Insert(cn, &channelLimitInfo{
			name:      cn,
			limits:    cl,
			isLiteral: util.IsChannelNameLiteral(cn),
		})
	}
	maxLevels := sublist.NumLevels()
	txt := []string{}
	title := "---------- Store Limits ----------"
	txt = append(txt, title)
	txt = append(txt, fmt.Sprintf("Channels:        %s",
		getLimitStr(true, int64(sl.MaxChannels),
			int64(DefaultStoreLimits.MaxChannels),
			limitCount)))
	maxLen := len(title)
	txt = append(txt, "--------- Channels Limits --------")
	txt = append(txt, getGlobalLimitsPrintLines(&sl.ChannelLimits)...)
	if len(sl.PerChannel) > 0 {
		channels := sublist.Subjects()
		channelLines := []string{}
		for _, cn := range channels {
			r := sublist.Match(cn)
			var prev *channelLimitInfo
			for i := 0; i < len(r); i++ {
				channel := r[i].(*channelLimitInfo)
				if channel.name == cn {
					var parentLimits *ChannelLimits
					if prev == nil {
						parentLimits = &sl.ChannelLimits
					} else {
						parentLimits = prev.limits
					}
					channelLines = append(channelLines,
						getChannelLimitsPrintLines(i, maxLevels, &maxLen, channel.name, channel.limits, parentLimits)...)
					break
				}
				prev = channel
			}
		}
		title := " List of Channels "
		numberDashesLeft := (maxLen - len(title)) / 2
		numberDashesRight := maxLen - len(title) - numberDashesLeft
		title = fmt.Sprintf("%s%s%s",
			repeatChar("-", numberDashesLeft),
			title,
			repeatChar("-", numberDashesRight))
		txt = append(txt, title)
		txt = append(txt, channelLines...)
	}
	txt = append(txt, repeatChar("-", maxLen))
	return txt
}

func getLimitStr(isGlobal bool, val, parentVal int64, limitType int) string {
	valStr := ""
	inherited := ""
	if !isGlobal && (val == parentVal) {
		return ""
	}
	if val == parentVal {
		inherited = " *"
	}
	if val == 0 {
		valStr = "unlimited"
	} else {
		switch limitType {
		case limitBytes:
			valStr = util.FriendlyBytes(val)
		case limitDuration:
			valStr = fmt.Sprintf("%v", time.Duration(val))
		default:
			valStr = fmt.Sprintf("%v", val)
		}
	}
	return fmt.Sprintf("%13s%s", valStr, inherited)
}

func getGlobalLimitsPrintLines(limits *ChannelLimits) []string {
	defaultLimits := &DefaultStoreLimits
	defMaxSubs := int64(defaultLimits.MaxSubscriptions)
	defMaxMsgs := int64(defaultLimits.MaxMsgs)
	defMaxBytes := defaultLimits.MaxBytes
	defMaxAge := defaultLimits.MaxAge
	defMaxInactivity := defaultLimits.MaxInactivity
	txt := []string{}
	txt = append(txt, fmt.Sprintf("  Subscriptions: %s", getLimitStr(true, int64(limits.MaxSubscriptions), defMaxSubs, limitCount)))
	txt = append(txt, fmt.Sprintf("  Messages     : %s", getLimitStr(true, int64(limits.MaxMsgs), defMaxMsgs, limitCount)))
	txt = append(txt, fmt.Sprintf("  Bytes        : %s", getLimitStr(true, limits.MaxBytes, defMaxBytes, limitBytes)))
	txt = append(txt, fmt.Sprintf("  Age          : %s", getLimitStr(true, int64(limits.MaxAge), int64(defMaxAge), limitDuration)))
	txt = append(txt, fmt.Sprintf("  Inactivity   : %s", getLimitStr(true, int64(limits.MaxInactivity), int64(defMaxInactivity), limitDuration)))
	return txt
}

func getChannelLimitsPrintLines(level, maxLevels int, maxLen *int, channelName string, limits, parentLimits *ChannelLimits) []string {
	plMaxSubs := int64(parentLimits.MaxSubscriptions)
	plMaxMsgs := int64(parentLimits.MaxMsgs)
	plMaxBytes := parentLimits.MaxBytes
	plMaxAge := parentLimits.MaxAge
	plMaxInactivity := parentLimits.MaxInactivity
	maxSubsOverride := getLimitStr(false, int64(limits.MaxSubscriptions), plMaxSubs, limitCount)
	maxMsgsOverride := getLimitStr(false, int64(limits.MaxMsgs), plMaxMsgs, limitCount)
	maxBytesOverride := getLimitStr(false, limits.MaxBytes, plMaxBytes, limitBytes)
	maxAgeOverride := getLimitStr(false, int64(limits.MaxAge), int64(plMaxAge), limitDuration)
	MaxInactivityOverride := getLimitStr(false, int64(limits.MaxInactivity), int64(plMaxInactivity), limitDuration)
	paddingLeft := repeatChar(" ", level)
	paddingRight := repeatChar(" ", maxLevels-level)
	txt := []string{}
	txt = append(txt, fmt.Sprintf("%s%s", paddingLeft, channelName))
	if maxSubsOverride != "" {
		txt = append(txt, fmt.Sprintf("%s |-> Subscriptions %s%s", paddingLeft, paddingRight, maxSubsOverride))
	}
	if maxMsgsOverride != "" {
		txt = append(txt, fmt.Sprintf("%s |-> Messages      %s%s", paddingLeft, paddingRight, maxMsgsOverride))
	}
	if maxBytesOverride != "" {
		txt = append(txt, fmt.Sprintf("%s |-> Bytes         %s%s", paddingLeft, paddingRight, maxBytesOverride))
	}
	if maxAgeOverride != "" {
		txt = append(txt, fmt.Sprintf("%s |-> Age           %s%s", paddingLeft, paddingRight, maxAgeOverride))
	}
	if MaxInactivityOverride != "" {
		txt = append(txt, fmt.Sprintf("%s |-> Inactivity    %s%s", paddingLeft, paddingRight, MaxInactivityOverride))
	}
	for _, l := range txt {
		if len(l) > *maxLen {
			*maxLen = len(l)
		}
	}
	return txt
}

func repeatChar(char string, len int) string {
	res := ""
	for i := 0; i < len; i++ {
		res += char
	}
	return res
}
