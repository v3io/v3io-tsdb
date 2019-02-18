#!/usr/bin/env python
# Copyright 2018 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from subprocess import run, PIPE
import re


with open('testdata/weather.csv') as fp:
    read_rows = sum(1 for _ in fp) - 1

# See benchmark_test.go
write_rows = 1982


out = run(['go', 'test', '-run', '^$', '-bench', '.'], stdout=PIPE)
if out.returncode != 0:
    raise SystemExit(1)

for line in out.stdout.decode('utf-8').splitlines():
    match = re.match(r'Benchmark(Read|Write)_([a-zA-Z]+).* (\d+) ns/op', line)
    if not match:
        continue
    op, proto, ns = match.groups()
    op, proto = op.lower(), proto.lower()
    us = int(ns) / 1000
    nrows = read_rows if op == 'read' else write_rows
    usl = us/nrows
    print(f'{proto:<5} {op:<5} {usl:7.3f}µs/row')
