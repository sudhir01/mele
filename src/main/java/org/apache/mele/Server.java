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
package org.apache.mele;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.mele.server.generated.*;
import org.apache.mele.thirdparty.thrift_0_9_0.TException;
import org.apache.zookeeper.ZooKeeper;

public class Server implements Mele.Iface {

  public static void main(String[] args) {

  }

  private final ZooKeeper _zooKeeper;
  private final Path _rootHdfsPath;

  public Server(ZooKeeper zooKeeper, Path rootHdfsPath) {
    _zooKeeper = zooKeeper;
    _rootHdfsPath = rootHdfsPath;
  }

  @Override
  public void enqueue(List<ByteBuffer> messages) throws MeleError, TException {

  }

  @Override
  public List<Message> dequeue(int max) throws MeleError, TException {
    return null;
  }

  @Override
  public void ack(List<ByteBuffer> ids) throws MeleError, TException {

  }

  @Override
  public void createQueue(String name) throws MeleError, TException {

  }

  @Override
  public void removeQueue(String name) throws MeleError, TException {

  }

  @Override
  public List<String> queueList() throws MeleError, TException {
    return null;
  }

}
