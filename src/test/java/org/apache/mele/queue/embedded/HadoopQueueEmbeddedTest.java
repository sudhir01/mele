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
package org.apache.mele.queue.embedded;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mele.Bytes;
import org.apache.mele.embedded.HadoopQueueEmbedded;
import org.apache.mele.embedded.HadoopQueueMessage;
import org.junit.Before;
import org.junit.Test;

public class HadoopQueueEmbeddedTest {

  private static Path rootPath = new Path("hdfs://localhost:9000/queue");
  private static Configuration configuration = new Configuration();

  @Before
  public void setup() throws IOException {
    FileSystem fileSystem = rootPath.getFileSystem(configuration);
    fileSystem.delete(rootPath, true);
    fileSystem.mkdirs(rootPath);
  }

  @Test
  public void testSingleEntry() throws IOException {
    Path file = new Path(rootPath, "file-" + UUID.randomUUID().toString());

    HadoopQueueEmbedded queue = new HadoopQueueEmbedded(file, configuration);
    Collection<byte[]> messages = new ArrayList<byte[]>();
    messages.add(Bytes.toBytes("abc"));
    queue.write(messages);

    Collection<HadoopQueueMessage> newMessages = new ArrayList<HadoopQueueMessage>();
    queue.read(100, newMessages);

    for (HadoopQueueMessage m : newMessages) {
      assertEquals("abc", Bytes.toString(m.getMessage()));
      queue.ack(Arrays.asList(m.getId()));
    }

    queue.close();
    assertRootPathEmpty();
  }

  @Test
  public void testSingleEntryWithFailure() throws IOException {
    Path file = new Path(rootPath, "file-" + UUID.randomUUID().toString());

    HadoopQueueEmbedded queue = new HadoopQueueEmbedded(file, configuration);
    Collection<byte[]> messages = new ArrayList<byte[]>();
    messages.add(Bytes.toBytes("abc"));
    queue.write(messages);
    queue.close();

    assertRootPathNotEmpty();
  }

  @Test
  public void testSingleEntryWithFailureAndReopen() throws IOException {
    Path file = new Path(rootPath, "file-" + UUID.randomUUID().toString());

    HadoopQueueEmbedded queue1 = new HadoopQueueEmbedded(file, configuration);
    Collection<byte[]> messages = new ArrayList<byte[]>();
    messages.add(Bytes.toBytes("abc"));
    queue1.write(messages);
    queue1.close();

    assertRootPathNotEmpty();

    HadoopQueueEmbedded queue2 = new HadoopQueueEmbedded(file, configuration);

    assertFalse(queue2.isWritable());
    assertTrue(queue2.isReadable());
    assertTrue(queue2.isAckable());

    queue2.close();

  }

  private void assertRootPathNotEmpty() throws IOException {
    FileSystem fileSystem = rootPath.getFileSystem(configuration);
    assertEquals(3, fileSystem.listStatus(rootPath).length);
  }

  @Test
  public void testBatchEntriesSmall() throws IOException {
    Path file = new Path(rootPath, "file-" + UUID.randomUUID().toString());

    HadoopQueueEmbedded queue = new HadoopQueueEmbedded(file, configuration);
    List<byte[]> messages = new ArrayList<byte[]>();
    create(10, messages);
    queue.write(messages);

    List<HadoopQueueMessage> newMessages = new ArrayList<HadoopQueueMessage>();
    queue.read(100, newMessages);

    assertEquals(10, newMessages.size());

    for (int i = 0; i < 10; i++) {
      HadoopQueueMessage message = newMessages.get(i);
      assertEquals(Bytes.toString(messages.get(i)), Bytes.toString(message.getMessage()));
      queue.ack(Arrays.asList(message.getId()));
    }

    queue.close();
    assertRootPathEmpty();
  }

  @Test
  public void testBatchEntriesSmallWithFailureAndReopen() throws IOException {
    Path file = new Path(rootPath, "file-" + UUID.randomUUID().toString());

    HadoopQueueEmbedded queue1 = new HadoopQueueEmbedded(file, configuration);
    List<byte[]> messages = new ArrayList<byte[]>();
    create(10, messages);
    queue1.write(messages);
    queue1.close();

    assertRootPathNotEmpty();

    HadoopQueueEmbedded queue2 = new HadoopQueueEmbedded(file, configuration);
    List<HadoopQueueMessage> newMessages = new ArrayList<HadoopQueueMessage>();
    queue2.read(100, newMessages);

    assertEquals(10, newMessages.size());

    for (int i = 0; i < newMessages.size(); i++) {
      HadoopQueueMessage message = newMessages.get(i);
      assertEquals(Bytes.toString(messages.get(i)), Bytes.toString(message.getMessage()));
      queue2.ack(Arrays.asList(message.getId()));
    }

    queue2.close();
    assertRootPathEmpty();
  }

  @Test
  public void testBatchEntriesSmallWithFailureAndReopenAcksAcrossFiles() throws IOException {
    Path file = new Path(rootPath, "file-" + UUID.randomUUID().toString());

    HadoopQueueEmbedded queue1 = new HadoopQueueEmbedded(file, configuration);
    List<byte[]> messages = new ArrayList<byte[]>();
    create(10, messages);
    queue1.write(messages);
    List<HadoopQueueMessage> newMessages1 = new ArrayList<HadoopQueueMessage>();
    queue1.read(2, newMessages1);
    for (HadoopQueueMessage m : newMessages1) {
      queue1.ack(Arrays.asList(m.getId()));
    }
    queue1.close();

    assertRootPathNotEmpty();

    HadoopQueueEmbedded queue2 = new HadoopQueueEmbedded(file, configuration);
    List<HadoopQueueMessage> newMessages = new ArrayList<HadoopQueueMessage>();
    queue2.read(100, newMessages);

    assertEquals(8, newMessages.size());

    for (int i = 0; i < newMessages.size(); i++) {
      HadoopQueueMessage message = newMessages.get(i);
      assertEquals(Bytes.toString(messages.get(i + 2)), Bytes.toString(message.getMessage()));
      queue2.ack(Arrays.asList(message.getId()));
    }

    queue2.close();
    assertRootPathEmpty();
  }

  @Test
  public void testBatchEntriesLarger() throws IOException {
    Path file = new Path(rootPath, "file-" + UUID.randomUUID().toString());

    HadoopQueueEmbedded queue = new HadoopQueueEmbedded(file, configuration);
    List<byte[]> messages = new ArrayList<byte[]>();
    int size = 1000;
    create(size, messages);
    queue.write(messages);

    int batchSize = 100;

    for (int b = 0; b < 10; b++) {
      List<HadoopQueueMessage> newMessages = new ArrayList<HadoopQueueMessage>();
      queue.read(batchSize, newMessages);
      assertEquals(batchSize, newMessages.size());
      int offset = b * batchSize;
      for (int i = 0; i < batchSize; i++) {
        HadoopQueueMessage message = newMessages.get(i);
        assertEquals(Bytes.toString(messages.get(i + offset)), Bytes.toString(message.getMessage()));
        queue.ack(Arrays.asList(message.getId()));
      }
    }

    queue.close();
    assertRootPathEmpty();
  }

  private void create(int size, Collection<byte[]> messages) {
    for (int i = 0; i < size; i++) {
      messages.add(UUID.randomUUID().toString().getBytes());
    }
  }

  private void assertRootPathEmpty() throws IOException {
    FileSystem fileSystem = rootPath.getFileSystem(configuration);
    assertEquals(0, fileSystem.listStatus(rootPath).length);
  }

}
