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

// convert duration string e.g. 24h to time (unix milisecond)
func Str2duration(duration string) (int64, error) {

	multiply := 3600 * 1000 // hour by default
	if len(duration) > 0 {
		last := duration[len(duration)-1:]
		if last == "m" || last == "h" || last == "d" || last == "y" {
			duration = duration[0 : len(duration)-1]
			switch last {
			case "m":
				multiply = 60 * 1000
			case "h":
				multiply = 3600 * 1000
			case "d":
				multiply = 24 * 3600 * 1000
			case "w":
				multiply = 7 * 24 * 3600 * 1000
			case "y":
				multiply = 365 * 24 * 3600 * 1000
			}
		}
	}

	if duration == "" {
		return 0, nil
	}

	i, err := strconv.Atoi(duration)
	if err != nil {
		return 0, errors.Wrap(err, "not a valid duration, use nn[s|h|m|d|y]")
	}

	return int64(i * multiply), nil
}

// convert time string to time (unix milisecond)
func Str2unixTime(tstr string) (int64, error) {

	if tstr == "now" || tstr == "now-" {
		return time.Now().Unix() * 1000, nil
	} else if strings.HasPrefix(tstr, "now-") {
		t, err := Str2duration(tstr[4:])
		if err != nil {
			return 0, errors.Wrap(err, "not a valid time 'now-??', 'now' need to follow with nn[s|h|m|d|y]")
		}
		return time.Now().Unix()*1000 - int64(t), nil
	}

	tint, err := strconv.Atoi(tstr)
	if err == nil {
		return int64(tint) * 1000, nil
	}

	t, err := time.Parse(time.RFC3339, tstr)
	if err != nil {
		return 0, errors.Wrap(err, "Not an RFC 3339 time format")
	}
	return t.Unix() * 1000, nil
}

func GetTimeFromRange(from, to, last, step string) (f int64, t int64, s int64, err error) {

	s, err = Str2duration(step)
	if err != nil {
		return
	}

	t = time.Now().Unix() * 1000
	if to != "" {
		t, err = Str2unixTime(to)
		if err != nil {
			return
		}
	}

	f = t - 1000*3600 // default of last hour
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
