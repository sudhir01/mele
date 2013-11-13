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

import java.io.UnsupportedEncodingException;

public class Bytes {

  private static final String UTF_8 = "UTF-8";

  public static long toLong(byte[] buf) {
    return toLong(buf, 0);
  }

  public static long toLong(byte[] buf, int off) {
    return ((buf[off + 7] & 0xFFL) << 0) + ((buf[off + 6] & 0xFFL) << 8) + ((buf[off + 5] & 0xFFL) << 16)
        + ((buf[off + 4] & 0xFFL) << 24) + ((buf[off + 3] & 0xFFL) << 32) + ((buf[off + 2] & 0xFFL) << 40)
        + ((buf[off + 1] & 0xFFL) << 48) + (((long) buf[off + 0]) << 56);
  }

  public static void toBytes(long val, byte[] buf, int off) {
    buf[off + 7] = (byte) (val >>> 0);
    buf[off + 6] = (byte) (val >>> 8);
    buf[off + 5] = (byte) (val >>> 16);
    buf[off + 4] = (byte) (val >>> 24);
    buf[off + 3] = (byte) (val >>> 32);
    buf[off + 2] = (byte) (val >>> 40);
    buf[off + 1] = (byte) (val >>> 48);
    buf[off + 0] = (byte) (val >>> 56);
  }

  public static int toInt(byte[] buf) {
    return toInt(buf, 0);
  }

  public static int toInt(byte[] buf, int off) {
    return ((buf[off + 3] & 0xFF) << 0) + ((buf[off + 2] & 0xFF) << 8) + ((buf[off + 1] & 0xFF) << 16)
        + ((buf[off + 0]) << 24);
  }

  public static void toBytes(int val, byte[] buf, int off) {
    buf[off + 3] = (byte) (val >>> 0);
    buf[off + 2] = (byte) (val >>> 8);
    buf[off + 1] = (byte) (val >>> 16);
    buf[off + 0] = (byte) (val >>> 24);
  }

  public static byte[] toBytes(String s) {
    try {
      return s.getBytes(UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static String toString(byte[] message) {
    try {
      return new String(message, UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
