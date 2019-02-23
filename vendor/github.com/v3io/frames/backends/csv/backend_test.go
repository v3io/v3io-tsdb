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

package csv

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/v3io/frames"
)

var (
	csvData = []byte(`STATION,DATE,PRCP,SNWD,SNOW,TMAX,TMIN,AWND,WDF2,WDF5,WSF2,WSF5,PGTM,FMTM
GHCND:USW00094728,2000-01-01,0,-9999,0,100,11,26,250,230,72,94,1337,1337.7
GHCND:USW00094728,2000-01-02,0,-9999,0,156,61,21,260,260,72,112,2313,2314.7
GHCND:USW00094728,2000-01-03,0,-9999,0,178,106,30,260,250,67,94,320,321.7
GHCND:USW00094728,2000-01-04,178,-9999,0,156,78,35,320,350,67,107,1819,1840.7
GHCND:USW00094728,2000-01-05,0,-9999,0,83,-17,51,330,340,107,143,843,844.7
GHCND:USW00094728,2000-01-06,0,-9999,0,56,-22,30,220,250,67,98,1833,1834.7
GHCND:USW00094728,2000-01-07,0,-9999,0,94,17,42,300,310,103,156,1521,1601.7
GHCND:USW00094728,2000-01-09,5,-9999,0,106,28,26,270,270,63,89,22,601.7
GHCND:USW00094728,2000-01-10,213,-9999,0,144,67,41,280,260,94,139,1736,1758.7
GHCND:USW00094728,2000-01-11,0,-9999,0,111,44,49,300,310,112,174,1203,1203.7
GHCND:USW00094728,2000-01-12,0,-9999,0,83,39,39,330,330,94,161,536,610.7
GHCND:USW00094728,2000-01-13,13,-9999,0,39,-78,51,90,10,103,143,1539,843.7
`)
	csvPath    string
	numCSVRows = 12
	numCSVCols = 14
	colTypes   = map[string]frames.DType{
		"STATION": frames.StringType,
		"DATE":    frames.TimeType,
		"PRCP":    frames.IntType,
		"SNWD":    frames.IntType,
		"FMTM":    frames.FloatType,
	}
)

func TestCSV(t *testing.T) {
	req := &frames.ReadRequest{}
	result := loadTempCSV(t, req)

	nRows := totalRows(result)
	if nRows != numCSVRows {
		t.Fatalf("# rows mismatch %d != %d", nRows, numCSVRows)
	}

	for _, frame := range result {
		if len(frame.Names()) != numCSVCols {
			t.Fatalf("# columns mismatch %d != %d", len(frame.Names()), numCSVRows)
		}

		for name, dtype := range colTypes {
			col, err := frame.Column(name)
			if err != nil {
				t.Fatalf("can't find column %q", name)
			}

			if col.DType() != dtype {
				t.Fatalf("dype mismatch %d != %d", dtype, col.DType())
			}
		}
	}

}

func TestLimit(t *testing.T) {
	limit := numCSVRows - 3

	req := &frames.ReadRequest{}
	req.Limit = int64(limit)

	result := loadTempCSV(t, req)
	if nRows := totalRows(result); nRows != limit {
		t.Fatalf("got %d rows, expected %d", nRows, limit)
	}
}

func TestMaxInMessage(t *testing.T) {
	frameLimit := numCSVRows / 3

	req := &frames.ReadRequest{}
	req.MessageLimit = int64(frameLimit)

	result := loadTempCSV(t, req)
	if nRows := totalRows(result); nRows != numCSVRows {
		t.Fatalf("got %d rows, expected %d", nRows, numCSVRows)
	}

	for _, frame := range result {
		if frame.Len() > frameLimit {
			t.Fatalf("frame too big (%d > %d)", frame.Len(), frameLimit)
		}
	}
}

func totalRows(result []frames.Frame) int {
	total := 0
	for _, frame := range result {
		total += frame.Len()
	}

	return total
}

func loadTempCSV(t *testing.T, req *frames.ReadRequest) []frames.Frame {
	logger, err := frames.NewLogger("debug")
	if err != nil {
		t.Fatalf("can't create logger - %s", err)
	}

	csvPath, err := tmpCSV()
	if err != nil {
		t.Fatal(err)
	}

	cfg := &frames.BackendConfig{
		Name:    "testCsv",
		Type:    "csv",
		RootDir: path.Dir(csvPath),
	}

	backend, err := NewBackend(logger, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	req.Table = path.Base(csvPath)
	it, err := backend.Read(req)
	if err != nil {
		t.Fatal(err)
	}

	var result []frames.Frame
	for it.Next() {
		result = append(result, it.At())
	}

	if err := it.Err(); err != nil {
		t.Fatal(err)
	}

	return result
}

func tmpCSV() (string, error) {
	tmp, err := ioutil.TempFile("", "csv-test")
	if err != nil {
		return "", err
	}

	_, err = tmp.Write(csvData)
	if err != nil {
		return "", nil
	}

	if err := tmp.Sync(); err != nil {
		return "", err
	}

	return tmp.Name(), nil
}
