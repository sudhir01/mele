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
package org.apache.mele.queue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mele.embedded.HadoopQueueEmbedded;
import org.apache.mele.embedded.HadoopQueueMessage;

public class ExampleUsageEmbedded {

  public static void main(String[] args) throws IOException, InterruptedException {
    waitForEnter();
    Path file = new Path("hdfs://localhost:9000/queue/file-" + UUID.randomUUID().toString());
    Configuration configuration = new Configuration();
//    configuration.setBoolean("ipc.client.tcpnodelay", true);
    // ipc.client.connection.maxidletime
    long fileSize = 1000000000L;
    final HadoopQueueEmbedded queue = new HadoopQueueEmbedded(file, configuration, fileSize);

//    final int batchSize = 10;
    // final int batchSize = 500;
    final int batchSize = 1000;
    final int size = 128;

    final AtomicLong writes = new AtomicLong();
    final AtomicLong reads = new AtomicLong();

    Thread write = new Thread(new Runnable() {
      @Override
      public void run() {
        Random random = new Random();
        List<byte[]> messages = new ArrayList<byte[]>();
        for (int i = 0; i < batchSize; i++) {
          byte[] buf = new byte[size];
          messages.add(buf);
        }
        while (queue.isWritable()) {
          for (int i = 0; i < batchSize; i++) {
            random.nextBytes(messages.get(i));
          }
          try {
            queue.write(messages);
            writes.addAndGet(messages.size());
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }
        System.out.println("Writing complete");
      }
    });
    write.start();

    Thread read = new Thread(new Runnable() {
      @Override
      public void run() {
        Collection<HadoopQueueMessage> newMessages = new ArrayList<HadoopQueueMessage>();
        while (queue.isReadable()) {
          newMessages.clear();
          try {
            queue.read(batchSize, newMessages);
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
          reads.addAndGet(newMessages.size());
          try {
            queue.ack(getIds(newMessages));
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }
        System.out.println("Reading complete");
      }

      private Collection<byte[]> getIds(Collection<HadoopQueueMessage> newMessages) {
        List<byte[]> ids = new ArrayList<byte[]>();
        for (HadoopQueueMessage m : newMessages) {
          ids.add(m.getId());
        }
        return ids;
      }
    });
    read.start();

    long pw = 0;
    long pr = 0;
    long start = System.nanoTime();

    while (!queue.isClosed()) {
      Thread.sleep(1000);
      int threadCount = ManagementFactory.getThreadMXBean().getThreadCount();
      long cw = writes.get();
      long cr = reads.get();

      double time = (System.nanoTime() - start) / 1000000000.0;
      double rrate = (cr - pr) / time;
      double wrate = (cw - pw) / time;

      System.out.printf("Writes [%14d @ %.1f /s] Reads [%14d @ %.1f /s] Threads [%4d]%n", cw, wrate, cr, rrate,
          threadCount);
      start = System.nanoTime();
      pw = cw;
      pr = cr;
    }

    queue.close();
  }

  private static void waitForEnter() throws IOException {
    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
    System.out.println("Hit enter to start...");
    bufferedReader.readLine();
  }
}
