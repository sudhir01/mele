/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace java org.apache.mele.server.generated

exception MeleError {
  1:string message,
  2:list<string> stacktrace
}

struct Message {
  1:binary id,
  2:binary message
}

service Mele {

  void enqueue(1:list<binary> messages) throws (1:MeleError ex)
  list<Message> dequeue(1:i32 max) throws (1:MeleError ex)
  void ack(1:list<binary> ids) throws (1:MeleError ex)

  void createQueue(1:string name) throws (1:MeleError ex)
  void removeQueue(1:string name) throws (1:MeleError ex)
  list<string> queueList() throws (1:MeleError ex)

}