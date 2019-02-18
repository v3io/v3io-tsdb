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
import json
from os import remove, path
import re

# From clients/py/Makefile
bench_json = '/tmp/framesd-py-bench.json'

with open('testdata/weather.csv') as fp:
    read_rows = sum(1 for _ in fp) - 1

# See clients/py/tests/test_benchmark.py
write_rows = 1982

if path.exists(bench_json):
    remove(bench_json)


out = run(['make', 'bench'], cwd='clients/py', stdout=PIPE)
if out.returncode != 0:
    raise SystemExit(out.stdout.decode('utf-8'))


with open(bench_json) as fp:
    data = json.load(fp)

for bench in data['benchmarks']:
    # test_read[http-csv]
    match = re.match(r'test_(\w+)\[([a-z]+)', bench['name'])
    if not match:
        raise SystemExit('error: bad test name - {}'.format(bench['name']))
    op, proto = match.groups()
    time_us = bench['stats']['mean'] * 1e6
    nrows = read_rows if op == 'read' else write_rows
    usl = time_us / nrows
    print(f'{proto:<5} {op:<5} {usl:7.3f}µs/row')
