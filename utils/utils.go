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

import "fmt"

func TimeToDHM(tmilli int64) (int, int) {
	t := int(tmilli / 1000)
	//m := t/60 - ((t/3600) * 60)
	h := t/3600 - ((t / 3600 / 24) * 24)
	d := t / 3600 / 24
	return d, h
}

func TimeToChunkId(dpo int, tmilli int64) (int, int) {
	d, h := TimeToDHM(tmilli)

	if dpo <= 1 {
		return h, 1
	}

	hoursPerChunk := HrPerChunk(dpo)
	dayIndex := d - ((d / dpo) * dpo)
	chunkIdx := dayIndex*24/hoursPerChunk + h/hoursPerChunk
	return chunkIdx, hoursPerChunk
}

// Get the recomended hours to store in a chunck based on number of days, will guarantee up to 100 attributes per object
func HrPerChunk(dpo int) int {
	switch {
	case dpo <= 1:
		return 1
	case dpo >= 2 && dpo <= 9:
		return dpo / 2
	case dpo >= 10 && dpo <= 15:
		return 4
	case dpo >= 16 && dpo <= 31:
		return 6
	}
	return 12
}

func Time2TableID(t int64) int {
	return 0
}

func Time2MinMax(dpo int, t int64) (int64, int64) {
	mint := (t / 3600 / 1000) * 3600 * 1000 // get nearest hour
	return mint, mint + 3600*1000*int64(HrPerChunk(dpo))
}

func Range2Cids(dpo int, mint, maxt int64) ([]int, int) {
	list := []int{}
	start, _ := TimeToChunkId(dpo, mint)
	end, _ := TimeToChunkId(dpo, maxt)

	for i := start; i < 24 || i <= end; i++ {
		list = append(list, i)
	}
	if end < start {
		for i := 0; i <= end; i++ {
			list = append(list, i)
		}

	}

	return list, HrPerChunk(dpo)
}

func ChunkID2Attr(col string, id, hr int) string {
	return fmt.Sprintf("__%s%d", col, id*hr)
}

func Range2Attrs(col string, dpo int, mint, maxt int64) []string {
	list, hrs := Range2Cids(dpo, mint, maxt)
	strList := []string{}
	for _, id := range list {
		strList = append(strList, ChunkID2Attr(col, id, hrs))
	}
	return strList
}
