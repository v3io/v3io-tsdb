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

package v3ioutils

import (
	"testing"
)

const schemaTst = `
{
  "fields": [
    {
      "name": "age",
      "type": "long",
      "nullable": true
    },
    {
      "name": "job",
      "type": "string",
      "nullable": true
    },
    {
      "name": "marital",
      "type": "string",
      "nullable": true
    },
    {
      "name": "education",
      "type": "string",
      "nullable": true
    },
    {
      "name": "default",
      "type": "string",
      "nullable": true
    },
    {
      "name": "balance",
      "type": "long",
      "nullable": true
    },
    {
      "name": "housing",
      "type": "string",
      "nullable": true
    },
    {
      "name": "loan",
      "type": "string",
      "nullable": true
    },
    {
      "name": "contact",
      "type": "string",
      "nullable": true
    },
    {
      "name": "day",
      "type": "long",
      "nullable": true
    },
    {
      "name": "month",
      "type": "string",
      "nullable": true
    },
    {
      "name": "duration",
      "type": "long",
      "nullable": true
    },
    {
      "name": "campaign",
      "type": "long",
      "nullable": true
    },
    {
      "name": "pdays",
      "type": "long",
      "nullable": true
    },
    {
      "name": "previous",
      "type": "long",
      "nullable": true
    },
    {
      "name": "poutcome",
      "type": "string",
      "nullable": true
    },
    {
      "name": "y",
      "type": "string",
      "nullable": true
    },
    {
      "name": "id",
      "type": "long",
      "nullable": false
    }
  ],
  "key": "id",
  "hashingBucketNum": 0
}
`

func TestNewSchema(t *testing.T) {
	schema, err := SchemaFromJSON([]byte(schemaTst))
	if err != nil {
		t.Fatal(err)
	}

	oldSchema, ok := schema.(*OldV3ioSchema)
	if !ok {
		t.Fatalf("can't get underlying schema object")
	}

	if nFields := len(oldSchema.Fields); nFields != 18 {
		t.Fatalf("wrong number of fields %d != %d", nFields, 18)
	}
}
