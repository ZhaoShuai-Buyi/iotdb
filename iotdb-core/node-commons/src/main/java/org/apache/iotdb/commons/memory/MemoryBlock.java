/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MemoryBlock extends IMemoryBlock {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryBlock.class);

  public MemoryBlock(
      final String name, final MemoryManager memoryManager, final long maxMemorySizeInByte) {
    this.name = name;
    this.memoryManager = memoryManager;
    this.maxMemorySizeInByte = maxMemorySizeInByte;
    this.memoryBlockType = MemoryBlockType.NONE;
  }

  public MemoryBlock(
      final String name,
      final MemoryManager memoryManager,
      final long maxMemorySizeInByte,
      final MemoryBlockType memoryBlockType) {
    this.name = name;
    this.memoryManager = memoryManager;
    this.maxMemorySizeInByte = maxMemorySizeInByte;
    this.memoryBlockType = memoryBlockType;
  }

  @Override
  public boolean allocate(long sizeInByte) {
    AtomicBoolean result = new AtomicBoolean(false);
    memoryUsageInBytes.updateAndGet(
        memCost -> {
          if (memCost + sizeInByte > maxMemorySizeInByte) {
            return memCost;
          }
          result.set(true);
          return memCost + sizeInByte;
        });
    return result.get();
  }

  @Override
  public boolean allocateUntilAvailable(long sizeInByte) throws InterruptedException {
    long originSize = memoryUsageInBytes.get();
    while (true) {
      boolean canUpdate = originSize + sizeInByte <= maxMemorySizeInByte;
      if (canUpdate && memoryUsageInBytes.compareAndSet(originSize, originSize + sizeInByte)) {
        break;
      }
      synchronized (memoryUsageInBytes) {
        memoryUsageInBytes.wait();
      }
      originSize = memoryUsageInBytes.get();
    }
    return true;
  }

  @Override
  public boolean allocateUntilAvailable(long sizeInByte, long timeInterval)
      throws InterruptedException {
    long originSize = memoryUsageInBytes.get();
    while (true) {
      boolean canUpdate = originSize + sizeInByte <= maxMemorySizeInByte;
      if (canUpdate && memoryUsageInBytes.compareAndSet(originSize, originSize + sizeInByte)) {
        break;
      }
      Thread.sleep(TimeUnit.MILLISECONDS.toMillis(timeInterval));
      originSize = memoryUsageInBytes.get();
    }
    return true;
  }

  @Override
  public void release(long sizeInByte) {
    memoryUsageInBytes.addAndGet(-sizeInByte);
  }

  @Override
  public void setMemoryUsageInBytes(long memoryUsageInBytes) {
    this.memoryUsageInBytes.set(memoryUsageInBytes);
  }

  @Override
  public String toString() {
    return "IoTDBMemoryBlock{"
        + "name="
        + name
        + ", isReleased="
        + isReleased
        + ", memoryBlockType="
        + memoryBlockType
        + ", maxMemorySizeInByte="
        + maxMemorySizeInByte
        + ", memoryUsageInBytes="
        + memoryUsageInBytes
        + '}';
  }

  @Override
  public void close() throws Exception {
    boolean isInterrupted = false;

    while (true) {
      try {
        if (lock.tryLock(100, TimeUnit.MICROSECONDS)) {
          try {
            memoryManager.release(this);
            if (isInterrupted) {
              LOGGER.warn("{} is released after thread interruption.", this);
            }
            break;
          } finally {
            lock.unlock();
          }
        }
      } catch (final InterruptedException e) {
        // Each time the close task is run, it means that the interrupt status left by the previous
        // tryLock does not need to be retained. Otherwise, it will lead to an infinite loop.
        isInterrupted = true;
        LOGGER.warn("Interrupted while waiting for the lock.", e);
      }
    }

    // Restore the interrupt status of the current thread
    if (isInterrupted) {
      Thread.currentThread().interrupt();
    }
  }
}
