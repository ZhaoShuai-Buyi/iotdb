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
package org.apache.iotdb.tsfile.file.metadata.statistics;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.PlainEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.SDTEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.ValuePoint;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

import java.util.ArrayList;
import java.util.List;

public class ValueIndex {

  double errorParam = TSFileDescriptor.getInstance().getConfig().getErrorParam();
  private DoubleArrayList values = new DoubleArrayList();
  public SDTEncoder sdtEncoder = new SDTEncoder();
  public double errorBound = 0;
  public PlainEncoder idxEncoder = new PlainEncoder(TSDataType.INT32, 0);
  public PlainEncoder valueEncoder = new PlainEncoder(TSDataType.DOUBLE, 0);
  public PublicBAOS idxOut = new PublicBAOS();
  public PublicBAOS valueOut = new PublicBAOS();
  public IntArrayList modelPointIdx_list = new IntArrayList();
  public DoubleArrayList modelPointVal_list = new DoubleArrayList();

  public List<ValuePoint> sortedModelPoints =
      new ArrayList<>(); // sorted by value in ascending order

  // this is necessary, otherwise serialized twice by timeseriesMetadata and chunkMetadata
  // causing learn() executed more than once!!
  private boolean isLearned = false;

  public int modelPointCount = 0; // except the first and last points

  private double stdDev = 0; // standard deviation of intervals
  private long count = 0;
  private double sumX2 = 0.0;
  private double sumX1 = 0.0;

  public void insert(int value) {
    values.add((double) value);
    count++;
    sumX1 += (double) value;
    sumX2 += (double) value * (double) value;
  }

  public void insert(long value) {
    values.add((double) value);
    count++;
    sumX1 += (double) value;
    sumX2 += (double) value * (double) value;
  }

  public void insert(float value) {
    values.add((double) value);
    count++;
    sumX1 += (double) value;
    sumX2 += (double) value * (double) value;
  }

  public void insert(double value) {
    values.add(value);
    count++;
    sumX1 += value;
    sumX2 += value * value;
  }

  public double getStdDev() { // sample standard deviation
    double std = Math.sqrt(this.sumX2 / this.count - Math.pow(this.sumX1 / this.count, 2));
    return Math.sqrt(Math.pow(std, 2) * this.count / (this.count - 1));
  }

  private void initForLearn() {
    this.stdDev = getStdDev();
    this.errorBound = 2 * stdDev * errorParam;
    this.sdtEncoder.setCompDeviation(errorBound / 2.0); // stdDev
  }

  public void learn() {
    if (isLearned) {
      // this is necessary, otherwise serialized twice by timeseriesMetadata and chunkMetadata
      // causing learn() executed more than once!!
      return;
    }
    isLearned = true;
    initForLearn(); // set self-adapting CompDeviation for sdtEncoder
    int pos = 0;
    boolean hasDataToFlush = false;
    for (double v : values.toArray()) {
      pos++; // starting from 1
      if (sdtEncoder.encodeDouble(pos, v)) {
        if (pos > 1) {
          // the first point value is stored as FirstValue in statistics, so here no need store the
          // first point
          // the last point won't be checked by the if SDT encode logic
          modelPointCount++;
          idxEncoder.encode((int) sdtEncoder.getTime(), idxOut);
          valueEncoder.encode(sdtEncoder.getDoubleValue(), valueOut);
          if (!hasDataToFlush) {
            hasDataToFlush = true;
          }
        }
      }
    }

    if (hasDataToFlush) {
      // otherwise no need flush, because GorillaV2 encoding will output NaN even if
      // hasDataToFlush=false
      idxEncoder.flush(idxOut); // necessary
      valueEncoder.flush(valueOut); // necessary
    }

    values = null; // raw values are not needed any more
  }
}
