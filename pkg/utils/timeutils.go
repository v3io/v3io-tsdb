/*
Copyright 2018 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/

package utils

import (
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"time"
)

const (
	OneMinuteMs = 60 * 1000
	OneHourMs   = 3600 * 1000
	OneDayMs    = 24 * 3600 * 1000
)

// Convert a "[0-9]+[mhd]" duration string (for example, "24h") to a
// Unix timestamp in milliseconds integer
func Str2duration(duration string) (int64, error) {

	multiply := OneHourMs // 1 hour by default
	if len(duration) > 0 {
		last := duration[len(duration)-1:]
		if last == "m" || last == "h" || last == "d" {
			duration = duration[0 : len(duration)-1]
			switch last {
			case "m":
				multiply = OneMinuteMs
			case "h":
				multiply = OneHourMs
			case "d":
				multiply = OneDayMs
			}
		}
	}

	if duration == "" {
		return 0, nil
	}

	i, err := strconv.Atoi(duration)
	if err != nil {
		return 0, errors.Wrap(err,
			`Invalid duration. Accepted pattern: [0-9]+[mhd]. Examples: "30d" (30 days); "5m" (5 minutes).`)
	}
	if i < 0 {
		return 0, errors.Errorf("The specified duration (%s) is negative.", duration)
	}

	return int64(i * multiply), nil
}

// Convert a time string to a Unix timestamp in milliseconds integer.
// The input time string can be of the format "now", "now-[0-9]+[mdh]" (for
// example, "now-2h"), "<Unix timestamp in milliseconds>", or "<RFC 3339 time>"
// (for example, "2018-09-26T14:10:20Z").
func Str2unixTime(timeString string) (int64, error) {
	if strings.HasPrefix(timeString, "now") {
		if len(timeString) > 3 {
			sign := timeString[3:4]
			duration := timeString[4:]

			t, err := Str2duration(duration)
			if err != nil {
				return 0, errors.Wrap(err, "Could not parse the pattern following 'now-'.")
			}
			if sign == "-" {
				return CurrentTimeInMillis() - int64(t), nil
			} else if sign == "+" {
				return CurrentTimeInMillis() + int64(t), nil
			} else {
				return 0, errors.Wrapf(err, "Unsupported time format:", timeString)
			}
		} else {
			return CurrentTimeInMillis(), nil
		}
	}

	tint, err := strconv.Atoi(timeString)
	if err == nil {
		return int64(tint), nil
	}

	t, err := time.Parse(time.RFC3339, timeString)
	if err != nil {
		return 0, errors.Wrap(err, "Invalid time string - not an RFC 3339 time format.")
	}
	return t.Unix() * 1000, nil
}

func CurrentTimeInMillis() int64 {
	return time.Now().Unix() * 1000
}

func GetTimeFromRange(from, to, last, step string) (f int64, t int64, s int64, err error) {

	s, err = Str2duration(step)
	if err != nil {
		return
	}

	t = CurrentTimeInMillis()
	if to != "" {
		t, err = Str2unixTime(to)
		if err != nil {
			return
		}
	}

	f = t - OneHourMs // default of last hour
	if from != "" {
		f, err = Str2unixTime(from)
		if err != nil {
			return
		}
	}

	if last != "" {
		var l int64
		l, err = Str2duration(last)
		if err != nil {
			return
		}
		f = t - l
	}

	return
}
