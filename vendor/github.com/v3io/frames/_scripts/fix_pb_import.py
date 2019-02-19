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


from argparse import ArgumentParser, FileType

parser = ArgumentParser(description='fix gRPC import')
parser.add_argument('file', help='file to fix', type=FileType('rt'))
args = parser.parse_args()

old_import = 'import frames_pb2 as frames__pb2'
new_import = 'from . import frames_pb2 as frames__pb2'

lines = []
for line in args.file:
    if line.startswith(old_import):
        lines.append(new_import + '\n')
    else:
        lines.append(line)
args.file.close()

with open(args.file.name, 'wt') as out:
    out.write(''.join(lines))
