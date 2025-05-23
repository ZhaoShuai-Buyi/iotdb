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

package org.apache.iotdb.commons.binaryallocator;

import org.apache.iotdb.commons.binaryallocator.arena.Arena;

import org.apache.tsfile.utils.PooledBinary;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;

public class PooledBinaryPhantomReference extends PhantomReference<PooledBinary> {
  public final byte[] byteArray;
  public Arena.SlabRegion slabRegion;

  public PooledBinaryPhantomReference(
      PooledBinary referent,
      ReferenceQueue<? super PooledBinary> q,
      byte[] byteArray,
      Arena.SlabRegion region) {
    super(referent, q);
    this.byteArray = byteArray;
    this.slabRegion = region;
  }
}
