package org.apache.iotdb.db.queryengine.plan.relational.function.tvf;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.TableSessionBuilder;
import org.junit.Test;

import java.util.Collections;

public class InsertElectricAndDocumentDataTest {

    @Test
    public void insertElectricAndDocumentData() {
        TEndPoint endPoint = IoTDBDescriptor.getInstance().getConfig().getAddressAndPort();
        String address = endPoint.ip + ":" + endPoint.port;
        String success = " is executed successfully";
        ITableSession session = null;
        try {
             session = new TableSessionBuilder()
                    .nodeUrls(Collections.singletonList(address))
                    .username("root")
                    .password("root")
                    .database("nx")
                    .build();
             try {
                 session.executeNonQueryStatement("create database nx");
                 System.out.println("create database nx" + success);
             } catch (Exception e) {
                 System.out.println("database nx is existed");
            }
             session.executeNonQueryStatement("use nx");
             try {
                 session.executeQueryStatement("desc electric");
                 session.executeNonQueryStatement("drop table electric");
                 System.out.println("drop current table electric" + success);
             } catch (Exception e) {
                 System.out.println("desc electric is not existed");
             } finally {
                 System.out.println("wait insert electric");
             }
            try {
                session.executeQueryStatement("desc document");
                session.executeNonQueryStatement("drop table document");
                System.out.println("drop current table document" + success);
            } catch (Exception e) {
                System.out.println("desc document is not existed");
            } finally {
                System.out.println("wait insert document");
            }
            session.executeNonQueryStatement("create table electric (time TIMESTAMP TIME, pn_id STRING TAG, data_property STRING TAG,"
            + " save_date TIMESTAMP FIELD, f1 FLOAT FIELD, f2 FLOAT FIELD)");
            session.executeNonQueryStatement("create table document (time TIMESTAMP TIME, id STRING TAG, station STRING FIELD, line STRING FIELD,"
            + " meter_id STRING FIELD, rv STRING FIELD, cali_cur STRING FIELD, rl STRING FIELD)");

            session.executeNonQueryStatement("insert into document values (1, '20007804', '徐家庄', '徐露线', '8200000021400466', '3×57.7/100V', '1(2)A', '83.088')");
            session.executeNonQueryStatement("insert into document values (1, '20009999', '华润变电站', '润一风线主', '8200000021500499', '3×380V', '13800/100', '3775680')");
            System.out.println("insert document" + success);

// ID: 20007804
            session.executeNonQueryStatement("insert into electric values (2025-12-01T00:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T00:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T00:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T00:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T01:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T01:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T01:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T01:45:00, '20007804', null, null, null, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T02:00:00, '20007804', null, null, 10, null)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T02:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T02:30:00, '20007804', null, null, 9, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T02:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T03:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T03:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T03:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T03:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T04:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T04:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T04:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T04:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T05:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T05:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T05:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T05:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T06:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T06:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T06:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T06:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T07:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T07:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T07:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T07:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T08:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T08:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T08:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T08:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T09:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T09:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T09:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T09:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T10:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T10:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T10:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T10:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T11:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T11:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T11:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T11:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T12:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T12:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T12:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T12:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T13:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T13:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T13:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T13:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T14:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T14:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T14:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T14:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T15:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T15:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T15:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T15:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T16:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T16:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T16:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T16:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T17:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T17:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T17:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T17:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T18:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T18:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T18:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T18:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T19:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T19:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T19:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T19:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T20:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T20:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T20:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T20:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T21:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T21:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T21:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T21:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T22:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T22:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T22:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T22:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T23:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T23:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T23:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T23:45:00, '20007804', null, null, 10, 15)");

            session.executeNonQueryStatement("insert into electric values (2025-12-02T00:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T00:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T00:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T00:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T01:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T01:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T01:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T01:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T02:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T02:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T02:30:00, '20007804', null, null, 8, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T02:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T03:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T03:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T03:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T03:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T04:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T04:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T04:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T04:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T05:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T05:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T05:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T05:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T06:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T06:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T06:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T06:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T07:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T07:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T07:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T07:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T08:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T08:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T08:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T08:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T09:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T09:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T09:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T09:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T10:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T10:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T10:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T10:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T11:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T11:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T11:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T11:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T12:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T12:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T12:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T12:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T13:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T13:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T13:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T13:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T14:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T14:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T14:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T14:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T15:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T15:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T15:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T15:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T16:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T16:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T16:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T16:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T17:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T17:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T17:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T17:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T18:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T18:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T18:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T18:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T19:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T19:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T19:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T19:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T20:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T20:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T20:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T20:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T21:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T21:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T21:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T21:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T22:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T22:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T22:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T22:45:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T23:00:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T23:15:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T23:30:00, '20007804', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T23:45:00, '20007804', null, null, 10, 15)");

// ID: 20009999
            //session.executeNonQueryStatement("insert into electric values (2025-12-01T00:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T00:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T00:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T00:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T01:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T01:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T01:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T01:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T02:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T02:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T02:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T02:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T03:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T03:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T03:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T03:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T04:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T04:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T04:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T04:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T05:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T05:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T05:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T05:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T06:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T06:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T06:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T06:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T07:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T07:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T07:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T07:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T08:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T08:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T08:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T08:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T09:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T09:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T09:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T09:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T10:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T10:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T10:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T10:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T11:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T11:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T11:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T11:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T12:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T12:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T12:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T12:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T13:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T13:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T13:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T13:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T14:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T14:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T14:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T14:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T15:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T15:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T15:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T15:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T16:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T16:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T16:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T16:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T17:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T17:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T17:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T17:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T18:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T18:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T18:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T18:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T19:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T19:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T19:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T19:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T20:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T20:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T20:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T20:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T21:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T21:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T21:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T21:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T22:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T22:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T22:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T22:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T23:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T23:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T23:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-01T23:45:00, '20009999', null, null, 10, 15)");

            session.executeNonQueryStatement("insert into electric values (2025-12-02T00:00:00, '20009999', null, null, 37900000, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T00:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T00:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T00:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T01:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T01:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T01:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T01:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T02:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T02:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T02:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T02:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T03:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T03:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T03:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T03:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T04:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T04:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T04:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T04:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T05:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T05:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T05:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T05:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T06:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T06:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T06:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T06:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T07:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T07:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T07:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T07:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T08:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T08:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T08:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T08:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T09:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T09:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T09:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T09:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T10:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T10:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T10:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T10:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T11:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T11:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T11:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T11:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T12:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T12:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T12:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T12:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T13:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T13:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T13:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T13:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T14:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T14:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T14:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T14:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T15:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T15:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T15:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T15:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T16:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T16:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T16:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T16:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T17:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T17:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T17:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T17:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T18:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T18:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T18:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T18:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T19:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T19:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T19:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T19:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T20:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T20:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T20:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T20:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T21:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T21:15:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T21:30:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T21:45:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T22:00:00, '20009999', null, null, 10, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T22:15:00, '20009999', null, null, null, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T22:30:00, '20009999', null, null, null, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T22:45:00, '20009999', null, null, null, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T23:00:00, '20009999', null, null, null, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T23:15:00, '20009999', null, null, null, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T23:30:00, '20009999', null, null, null, 15)");
            session.executeNonQueryStatement("insert into electric values (2025-12-02T23:45:00, '20009999', null, null, null, 15)");

            System.out.println("insert electric" + success);
            System.out.println("insert all finished");
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (session != null) {
                    session.close();
                }
            } catch (IoTDBConnectionException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Test
    public void useFunction() {
        TEndPoint endPoint = IoTDBDescriptor.getInstance().getConfig().getAddressAndPort();
        String address = endPoint.ip + ":" + endPoint.port;
        String success = " is executed successfully";
        ITableSession session = null;
        try {
            session = new TableSessionBuilder()
                    .nodeUrls(Collections.singletonList(address))
                    .username("root")
                    .password("root")
                    .database("nx")
                    .build();
        } catch (IoTDBConnectionException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (session != null) {
                    session.close();
                }
            } catch (IoTDBConnectionException e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
