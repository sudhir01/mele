#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

rm ./src/main/java/org/apache/mele/server/generated/*.java
thrift -out ./src/main/java/ -gen java Mele.thrift
for f in ./src/main/java/org/apache/mele/server/generated/*.java; do
  sed 's/org\.apache\.thrift\./org\.apache\.mele\.thirdparty\.thrift_0_9_0\./g' $f > $f.new2
  sed 's/import\ org\.slf4j\.Logger/\/\/import\ org\.slf4j\.Logger/g' $f.new2 > $f.new3
  sed 's/private\ static\ final\ Logger\ LOGGER/\/\/private\ static\ final\ Logger\ LOGGER/g' $f.new3 > $f.new4
  rm $f.new2 $f.new3 $f
  mv $f.new4 $f
done
