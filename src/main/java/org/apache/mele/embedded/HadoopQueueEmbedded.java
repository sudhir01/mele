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
package org.apache.mele.embedded;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.DFSInputStream;

public class HadoopQueueEmbedded implements HadoopQueue {

  private static final Log LOG = LogFactory.getLog(HadoopQueueEmbedded.class);

  private static final String FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
  private final Configuration _conf;
  private final Path _file;
  private final List<Path> _ackFiles = new ArrayList<Path>();
  private final AtomicLong _consumer = new AtomicLong();
  private final AtomicLong _producer = new AtomicLong();
  private final FSDataOutputStream _queueOutputStream;
  private final AtomicBoolean _isWritingClosed = new AtomicBoolean(false);
  private final AtomicBoolean _isReadingClosed = new AtomicBoolean(false);
  private final AtomicBoolean _isAckClosed = new AtomicBoolean(false);
  private final AtomicBoolean _isAckStarted = new AtomicBoolean(false);
  private final AtomicBoolean _shutdown = new AtomicBoolean(false);
  private final long _maxAllowed;
  private final FileSystem _queueWritingFileSystem;
  private final FileSystem _ackWritingFileSystem;
  private final Lock _writeLock = new ReentrantReadWriteLock().writeLock();
  private final Lock _readLock = new ReentrantReadWriteLock().writeLock();
  private final Lock _ackLock = new ReentrantReadWriteLock().writeLock();
  private final Thread _ackThread;
  private final long _ackSleepTime;
  private final FollowingFile _queueReading;
  private final ReaderPointer _readerPointer;

  private Path _currentAckFile;
  private FSDataOutputStream _ackOutputStream;

  public HadoopQueueEmbedded(Path file, Configuration configuration) throws IOException {
    this(file, configuration, 64 * 1024 * 1024);
  }

  public HadoopQueueEmbedded(Path file, Configuration configuration, long maxAllowed) throws IOException {
    _file = file;
    _conf = configuration;
    _maxAllowed = maxAllowed;
    // Will need to close and reopen file system objects. So no caching.
    _conf.setBoolean(FS_HDFS_IMPL_DISABLE_CACHE, true);

    _readerPointer = new ReaderPointer(_conf, _file);
    _consumer.set(_readerPointer.getReaderPosition());

    _queueReading = new FollowingFile(_conf, newFileSystem(_file), _file);

    _ackWritingFileSystem = newFileSystem(file.getParent());

    reopenAckFile(_ackWritingFileSystem);

    if (!_queueReading._fileSystem.exists(_file)) {
      _queueWritingFileSystem = newFileSystem(_file);
      _queueOutputStream = _queueWritingFileSystem.create(_file, false);
      _isWritingClosed.set(false);
    } else {
      _queueWritingFileSystem = null;
      _queueOutputStream = null;
      _isWritingClosed.set(true);
      FileStatus fileStatus = _queueReading._fileSystem.getFileStatus(_file);
      _producer.set(fileStatus.getLen());
    }
    _ackSleepTime = 1000;
    _ackThread = getAckThread();
  }

  private void reopenAckFile(FileSystem fileSystem) throws IOException {
    int ackFileCount = 0;
    while (true) {
      Path ackFile = new Path(_file.getParent(), _file.getName() + ".ack." + buffer(10, ackFileCount));
      _ackFiles.add(ackFile);
      if (!fileSystem.exists(ackFile)) {
        _currentAckFile = ackFile;
        _ackOutputStream = fileSystem.create(_currentAckFile, false);
        break;
      }
      ackFileCount++;
    }
  }

  private String buffer(int bufSize, int count) {
    String s = Integer.toString(count);
    int bufAmount = s.length() - bufSize;
    StringBuffer buffer = new StringBuffer();
    for (int i = 0; i < bufAmount; i++) {
      buffer.append('0');
    }
    buffer.append(s);
    return buffer.toString();
  }

  private Thread getAckThread() {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        LOG.info("Starting ack thread.");
        while (!_shutdown.get()) {
          try {
            if (ackCheck()) {
              try {
                closeAck();
              } catch (IOException e) {
                LOG.error("Unknown error during closing of ack.", e);
              }
              _shutdown.set(true);
              return;
            }
          } catch (IOException e) {
            LOG.error("Unknown error during ack check.", e);
          }
          try {
            sleep(_ackSleepTime);
          } catch (IOException e) {
            LOG.error("Unknown error during sleep.", e);
          }
        }
      }
    });
    thread.setName("ACK THREAD [" + _currentAckFile + "]");
    return thread;
  }

  @Override
  public void read(int batch, Collection<HadoopQueueMessage> messages) throws IOException {
    _readLock.lock();
    try {
      long prod = _producer.get();
      checkForReadClosing();
      while (_consumer.get() < prod) {
        messages.add(readMessage());
        if (messages.size() >= batch) {
          return;
        }
      }
    } finally {
      if (isReadable()) {
        _readerPointer.recordPosition(_consumer.get());
      }
      _readLock.unlock();
    }
  }

  @Override
  public void write(Collection<byte[]> messages) throws IOException {
    if (!isWritable()) {
      throw new IOException("Not open for writing.");
    }
    _writeLock.lock();
    try {
      for (byte[] message : messages) {
        _queueOutputStream.writeInt(message.length);
        _queueOutputStream.write(message);
      }
      flushAndSync(_queueOutputStream);
      _producer.set(_queueOutputStream.getPos());
      checkForWriteClosing();
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public void ack(Collection<byte[]> ids) throws IOException {
    _ackLock.lock();
    try {
      for (byte[] id : ids) {
        _ackOutputStream.write(id);
      }
      flushAndSync(_ackOutputStream);
      checkForAckCheckStart();
    } finally {
      _ackLock.unlock();
    }
  }

  private void checkForAckCheckStart() {
    if (!_isAckStarted.get() && !isReadable()) {
      _isAckStarted.set(true);
      _ackThread.start();
    }
  }

  @Override
  public boolean isWritable() {
    if (_queueOutputStream == null || _isWritingClosed.get()) {
      return false;
    }
    return true;
  }

  @Override
  public boolean isReadable() {
    if (isWritable()) {
      return true;
    }
    return !_isReadingClosed.get();
  }

  @Override
  public boolean isAckable() {
    if (_isAckClosed.get()) {
      return false;
    }
    return true;
  }

  @Override
  public boolean isClosed() {
    if (!isWritable() && !isReadable() && !isAckable()) {
      return true;
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    try {
      closeWriter();
    } catch (Exception e) {
      LOG.error("Error while trying to close writer.", e);
    }
    try {
      closeReader();
    } catch (Exception e) {
      LOG.error("Error while trying to close reader.", e);
    }
    try {
      closeAck();
    } catch (Exception e) {
      LOG.error("Error while trying to close ack.", e);
    }
    tryToCheckAck();
    _shutdown.set(true);
  }

  private void tryToCheckAck() throws IOException {
    if (_producer.get() == _consumer.get()) {
      if (ackCheck()) {
        LOG.info("All messages accounted for removing files [" + _file + "], [" + _ackFiles + "].");
        removeAllFiles();
      }
    }
  }

  private void closeAck() throws IOException {
    if (!_isAckClosed.get()) {
      _ackOutputStream.close();
      _ackWritingFileSystem.close();
      _isAckClosed.set(true);
    }
  }

  private void removeAllFiles() throws IOException {
    FileSystem fileSystem = newFileSystem(_file);
    fileSystem.delete(_file, false);
    _readerPointer.removeFile(fileSystem);
    for (Path path : _ackFiles) {
      fileSystem.delete(path, false);
    }
    fileSystem.close();
  }

  private void checkForWriteClosing() throws IOException {
    if (_producer.get() >= _maxAllowed) {
      closeWriter();
    }
  }

  private void closeWriter() throws IOException {
    if (!_isWritingClosed.get()) {
      _isWritingClosed.set(true);
      _queueOutputStream.close();
      _queueWritingFileSystem.close();
    }
  }

  private void checkForReadClosing() throws IOException {
    if (_consumer.get() == _producer.get() && !isWritable()) {
      closeReader();
    }
  }

  private void closeReader() throws IOException {
    if (!_isReadingClosed.get()) {
      _isReadingClosed.set(true);
      _queueReading.close();
      _readerPointer.close();
    }
  }

  private HadoopQueueMessage readMessage() throws IOException {
    long position = _consumer.get();
    final long originalPosition = position;
    long fileLength = -1L;
    if (_queueReading._inputStream != null) {
      fileLength = _queueReading._inputStream.getFileLength();
    }
    _queueReading.reopenQueueReaderIfNeeded(position + 4);
    try {
      _queueReading.seek(position);
    } catch (IOException e) {
      LOG.error("Seek failed position [" + position + "] file length [" + fileLength + "]", e);
      throw e;
    }
    final int length = readInt(_queueReading._inputStream);
    position += 4;
    _queueReading.reopenQueueReaderIfNeeded(position + length);
    _queueReading.seek(position);
    byte[] buf = new byte[length];
    int off = 0;
    int len = length;
    while (len > 0) {
      int read = _queueReading._inputStream.read(buf, off, len);
      len -= read;
      off += read;
      position += read;
    }
    _consumer.set(position);
    return new HadoopQueueMessage(originalPosition, length, buf);
  }

  private static void sleep(long t) throws IOException {
    try {
      Thread.sleep(t);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private boolean ackCheck() throws IOException {
    LOG.info("Starting ack check");
    BitSet bitSet = new BitSet();
    FileSystem fileSystem = null;
    try {
      _ackLock.lock();
      _ackOutputStream.close();
      fileSystem = newFileSystem(_file);
      FileStatus fileStatus = fileSystem.getFileStatus(_file);
      long dataLength = fileStatus.getLen();
      long totalAckLength = getTotalAckLength(fileSystem);
      if (!couldContainAllAcks(totalAckLength)) {
        LOG.info("Existing early [" + totalAckLength + "] because [" + totalAckLength % 12 + "]");
        return false;
      }
      for (Path ackFile : _ackFiles) {
        LOG.info("Starting ack check for file [" + ackFile + "]");
        DFSInputStream inputStream = null;
        try {
          inputStream = getDFS(fileSystem.open(ackFile));
          long length = inputStream.getFileLength();
          DataInputStream dataInputStream = new DataInputStream(inputStream);
          while (length > 0) {
            int pos = (int) dataInputStream.readLong();
            // @TODO check position
            // 4 bytes for storing the length of the message
            int len = dataInputStream.readInt() + 4;
            bitSet.set(pos, pos + len);
            length -= 12;
          }
          if (bitSet.cardinality() == dataLength) {
            return true;
          }
        } finally {
          if (inputStream != null) {
            inputStream.close();
          }
        }
      }
      return false;
    } finally {
      reopenAckFile(fileSystem);
      _ackLock.unlock();
      if (fileSystem != null) {
        fileSystem.close();
      }
    }
  }

  private long getTotalAckLength(FileSystem fileSystem) throws IOException {
    long size = 0;
    for (Path ackFile : _ackFiles) {
      DFSInputStream inputStream = getDFS(fileSystem.open(ackFile));
      size += inputStream.getFileLength();
      inputStream.close();
    }
    return size;
  }

  private boolean couldContainAllAcks(long totalAckLength) {
    if (totalAckLength == 0) {
      return false;
    }
    if (totalAckLength % 12 != 0) {
      return false;
    }

    // @TODO count number of messages and see if ack files could contain all
    // acks.
    return true;
  }

  private void flushAndSync(FSDataOutputStream out) throws IOException {
    out.flush();
    out.sync();
  }

  private static FileSystem newFileSystem(Path p, Configuration configuration) throws IOException {
    return FileSystem.get(p.toUri(), configuration);
  }

  private FileSystem newFileSystem(Path p) throws IOException {
    return newFileSystem(p, _conf);
  }

  static class FollowingFile implements Closeable {
    DFSInputStream _inputStream;
    FileSystem _fileSystem;
    Path _path;
    Configuration _conf;

    FollowingFile(Configuration configuration, FileSystem fileSystem, Path path) {
      _conf = configuration;
      _fileSystem = fileSystem;
      _path = path;
    }

    void seek(long position) throws IOException {
      if (_inputStream.getPos() == position) {
        return;
      }
      _inputStream.seek(position);
    }

    void reopenQueueReaderIfNeeded(long positionToReadTo) throws IOException {
      if (_inputStream == null) {
        _inputStream = getDFS(_fileSystem.open(_path));
        return;
      }
      long currentPos = _inputStream.getPos();
      int count = 1;
      do {
        if (positionToReadTo < _inputStream.getFileLength()) {
          return;
        }
        _inputStream.close();
        sleep(10 * count);
        while (true) {
          try {
            _inputStream = getDFS(_fileSystem.open(_path));
            break;
          } catch (IOException e) {
            LOG.warn("Got stange error [" + e.getMessage() + "]");
            _fileSystem.close();
            sleep(10);
            _fileSystem = newFileSystem(_path, _conf);
            _inputStream = getDFS(_fileSystem.open(_path));
          }
        }
        count++;
      } while (positionToReadTo > _inputStream.getFileLength());
      _inputStream.seek(currentPos);
    }

    @Override
    public void close() throws IOException {
      if (_inputStream != null) {
        _inputStream.close();
      }
      _fileSystem.close();
    }
  }

  private static DFSInputStream getDFS(FSDataInputStream inputStream) throws IOException {
    try {
      Field field = FilterInputStream.class.getDeclaredField("in");
      field.setAccessible(true);
      return (DFSInputStream) field.get(inputStream);
    } catch (NoSuchFieldException e) {
      throw new IOException(e);
    } catch (SecurityException e) {
      throw new IOException(e);
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  private static int readInt(InputStream inputStream) throws IOException {
    int i1 = inputStream.read();
    int i2 = inputStream.read();
    int i3 = inputStream.read();
    int i4 = inputStream.read();
    if ((i1 | i2 | i3 | i4) < 0) {
      throw new EOFException();
    }
    return ((i1 << 24) + (i2 << 16) + (i3 << 8) + (i4 << 0));
  }

  static class ReaderPointer implements Closeable {

    private long _position;
    private FileSystem _fileSystem;
    private FSDataOutputStream _outputStream;
    private Path _path;

    ReaderPointer(Configuration configuration, Path path) throws IOException {
      _path = new Path(path.getParent(), path.getName() + ".pointer");
      _fileSystem = newFileSystem(_path, configuration);
      if (_fileSystem.exists(_path)) {
        FSDataInputStream inputStream = _fileSystem.open(_path);
        DFSInputStream dfs = getDFS(inputStream);
        long fileLength = dfs.getFileLength();
        long offset = fileLength % 8;
        long filePostion = fileLength - offset - 8;
        if (filePostion >= 0) {
          inputStream.seek(filePostion);
          _position = inputStream.readLong();
        }
        inputStream.close();
      }
      _outputStream = _fileSystem.create(_path, true);
      _outputStream.writeLong(_position);
      _outputStream.flush();
      _outputStream.sync();
    }

    public void removeFile(FileSystem fileSystem) throws IOException {
      fileSystem.delete(_path, false);
    }

    long getReaderPosition() {
      return _position;
    }

    void recordPosition(long position) throws IOException {
      _position = position;
      _outputStream.writeLong(_position);
      _outputStream.flush();
      _outputStream.sync();
    }

    @Override
    public void close() throws IOException {
      _outputStream.close();
      _fileSystem.close();
    }

  }

}
