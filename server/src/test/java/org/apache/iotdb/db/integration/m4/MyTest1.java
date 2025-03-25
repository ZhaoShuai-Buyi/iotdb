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

package org.apache.iotdb.db.integration.m4;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBStatement;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;

import static org.junit.Assert.fail;

public class MyTest1 {

  private static final String TIMESTAMP_STR = "Time";

  private static String[] creationSqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle.d0",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT64, ENCODING=PLAIN",
        // IoTDB int data type does not support plain encoding, so use long data type
      };

  private final String d0s0 = "root.vehicle.d0.s0";

  private static final String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0)" + " VALUES(%d,%d)";

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static boolean originalEnableCPV;
  private static boolean originalUseChunkIndex;

  private static boolean originalUseMad;
  private static CompactionStrategy originalCompactionStrategy;

  @Before
  public void setUp() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("PLAIN");
    originalCompactionStrategy = config.getCompactionStrategy();
    config.setCompactionStrategy(CompactionStrategy.NO_COMPACTION);

    originalEnableCPV = config.isEnableCPV();
    config.setEnableCPV(true); // CPV

    originalUseMad = TSFileDescriptor.getInstance().getConfig().isUseMad();
    TSFileDescriptor.getInstance().getConfig().setUseMad(true);

    TSFileDescriptor.getInstance().getConfig().setErrorParam(10);

    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    config.setTimestampPrecision("ms");
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    config.setCompactionStrategy(originalCompactionStrategy);
    config.setEnableCPV(originalEnableCPV);
    //    TSFileDescriptor.getInstance().getConfig().setUseTimeIndex(originalUseChunkIndex);
    TSFileDescriptor.getInstance().getConfig().setUseMad(originalUseMad);
  }

  @Test
  public void test1() throws Exception {
    prepareData1();

    String[] res =
        new String[] {
          "0,1,20,5,20,5[1],30[10]",
          "25,25,45,8,30,8[25],40[30]",
          "50,52,54,8,18,8[52],18[54]",
          "75,null,null,null,null,null,null"
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      /*
       * Sql format: SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0),
       * max_value(s0) ROM root.xx group by ([tqs,tqe),IntervalLength). Requirements: (1) Don't change the
       * sequence of the above six aggregates (2) Make sure (tqe-tqs) is divisible by IntervalLength. (3)
       * Assume each chunk has only one page.
       */
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
      System.out.println(((IoTDBStatement) statement).executeFinish());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void test5() throws Exception {
    prepareData5();

    String[] res =
        new String[] {
          "0,1,20,5,20,5[1],30[10]",
          "25,25,45,8,30,8[25],40[30]",
          "50,null,null,null,null,null,null",
          "75,null,null,null,null,null,null"
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData1() {
    // data:
    // https://user-images.githubusercontent.com/33376433/151985070-73158010-8ba0-409d-a1c1-df69bad1aaee.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 25, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 54, 3));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 120, 8));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 20));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 27, 20));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 40));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 35, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 20));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 33, 9));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 45, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 52, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 54, 18));
      statement.execute("FLUSH");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void prepareData5() {
    // data:
    // https://user-images.githubusercontent.com/33376433/151985070-73158010-8ba0-409d-a1c1-df69bad1aaee.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 25, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 54, 3));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 120, 8));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 20));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 27, 20));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 40));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 35, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 20));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 33, 9));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 45, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 52, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 54, 18));
      statement.execute("FLUSH");

      statement.execute("delete from root.vehicle.d0.s0 where time>=52 and time<=54");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test2() { // add deletes
    prepareData2();

    String[] res =
        new String[] {
          "0,1,20,5,20,5[1],30[10]",
          "25,25,27,8,20,8[25],20[27]",
          "50,null,null,null,null,null,null",
          "75,null,null,null,null,null,null"
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)"); // don't change the
      // sequence!!!

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData2() {
    // data:
    // https://user-images.githubusercontent.com/33376433/151995378-07a2f8df-5cac-499a-ae88-e3b017eee07a.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 25, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 54, 3));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 120, 8));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 20));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 27, 20));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 40));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 35, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 20));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 33, 9));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 45, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 52, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 54, 18));
      statement.execute("FLUSH");

      statement.execute("delete from root.vehicle.d0.s0 where time>=28 and time<=60");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test2_2() { // use data2 but change the sql from group by ([0,100),25ms) to group by
    // ([0,150),25ms)
    prepareData2();

    String[] res =
        new String[] {
          "0,1,20,5,20,5[1],30[10]",
          "25,25,27,8,20,8[25],20[27]",
          "50,null,null,null,null,null,null",
          "75,null,null,null,null,null,null",
          "100,120,120,8,8,8[120],8[120]",
          "125,null,null,null,null,null,null"
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,150),25ms)"); // don't change the
      // sequence!!!

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void test3() { // all seq
    prepareData3();

    String[] res =
        new String[] {
          "0,1,22,5,4,1[10],10[2]",
          "25,30,40,8,2,2[40],8[30]",
          "50,55,72,5,4,4[72],20[62]",
          "75,80,90,11,1,1[90],11[80]"
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)"); // don't change the
      // sequence!!!

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData3() {
    // data:
    // https://user-images.githubusercontent.com/33376433/152003603-6b4e7494-00ff-47e4-bf6e-cab3c8600ce2.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 5));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 22, 4));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 2));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 55, 5));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 60, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 62, 20));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 65, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 70, 18));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 72, 4));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 80, 11));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 90, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 105, 7));
      statement.execute("FLUSH");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test3_2() { // not seq but no overlap
    prepareData3_2();

    String[] res =
        new String[] {
          "0,1,22,5,4,1[10],10[2]",
          "25,30,40,8,2,2[40],8[30]",
          "50,55,72,5,4,4[72],20[62]",
          "75,80,90,11,1,1[90],11[80]"
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)"); // don't change the
      // sequence!!!

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData3_2() {
    // data:
    // https://user-images.githubusercontent.com/33376433/152003603-6b4e7494-00ff-47e4-bf6e-cab3c8600ce2.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 72, 4));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 80, 11));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 90, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 105, 7));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 5));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 22, 4));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 2));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 55, 5));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 60, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 62, 20));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 65, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 70, 18));
      statement.execute("FLUSH");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test4() { // delete sequence move forward
    prepareData4();

    String[] res =
        new String[] {
          "0,1,20,5,20,5[1],30[10]",
          "25,25,45,8,30,8[25],30[45]",
          "50,52,54,8,18,8[52],18[54]",
          "75,null,null,null,null,null,null"
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)"); // don't change the
      // sequence!!!

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(String.format("min_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_time(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("first_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("last_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("min_value(%s)", d0s0))
                  + ","
                  + resultSet.getString(String.format("max_value(%s)", d0s0));
          System.out.println(ans);
          Assert.assertEquals(res[i++], ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData4() {
    // data:
    // https://user-images.githubusercontent.com/33376433/152006061-f1d95952-3f5c-4d88-b34e-45d3bb61b600.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 25, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 54, 3));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 120, 8));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 5, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 8, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 20));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 27, 20));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 40));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 35, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 20));
      statement.execute("FLUSH");

      statement.execute("delete from root.vehicle.d0.s0 where time>=28 and time<=42");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 33, 9));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 45, 30));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 52, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 54, 18));
      statement.execute("FLUSH");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
