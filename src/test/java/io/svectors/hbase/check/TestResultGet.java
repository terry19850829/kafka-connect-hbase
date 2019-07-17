package io.svectors.hbase.check;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by terry on 2019/7/16.
 */
public class TestResultGet {

    private static final String ZK_QUORUM = "hb-bp1z4glml2rrm7q46-master1-001.hbase.rds.aliyuncs.com:2181,hb-bp1z4glml2rrm7q46-master3-001.hbase.rds.aliyuncs.com:2181,hb-bp1z4glml2rrm7q46-master2-001.hbase.rds.aliyuncs.com:2181";
    private static final String TABLE_NAME = "upay_core_merchant";
    private static final String CF_DEFAULT = "cf";


    @Test
    public void testResultScan() {

        final Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, ZK_QUORUM);

        Connection connection = null;

        try {

            connection = ConnectionFactory.createConnection(configuration);
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            tableDescriptor.addFamily(new HColumnDescriptor(CF_DEFAULT));

//            System.out.print("Creating table. ");
//            Admin admin = connection.getAdmin();
//            admin.createTable(tableDescriptor);
//            System.out.println(" Done.");

            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            try {


//                Scan scan = new Scan();
//                scan.setFilter(new PageFilter(50));
//                SingleColumnValueFilter scvf = new SingleColumnValueFilter(CF_DEFAULT.getBytes(), "extra".getBytes(), CompareFilter.CompareOp.NOT_EQUAL, "0".getBytes());
//                scvf.setFilterIfMissing(true);
//                scan.setFilter(scvf);
//
//                ResultScanner scanner = table.getScanner(scan);
//                for (Result result : scanner) {
//                    System.out.println(new String(result.getRow()));
//                    byte[] b = result.getValue(CF_DEFAULT.getBytes(), "extra".getBytes());
//                    System.out.println(new String(b, "utf8"));
//                }

                Get get = new Get("01106e4a5b20-cd1b-6004-05c0-f79983ad".getBytes());
                Result r = table.get(get);
                if (r.isEmpty()) {
                    System.out.println("result is null");
                } else {
                    List<Cell> cellList = r.listCells();
                    for (Cell c : cellList) {
                        System.out.println(Bytes.toString(CellUtil.cloneQualifier(c)) + ":" + Bytes.toString(CellUtil.cloneValue(c)));
                    }
                    System.out.println("ctime:" + Bytes.toLong(r.getValue(CF_DEFAULT.getBytes(), "ctime".getBytes())));
                    System.out.println("withdraw_mode:" + Bytes.toInt(r.getValue(CF_DEFAULT.getBytes(), "withdraw_mode".getBytes())));
                    System.out.println("rank:" + Bytes.toInt(r.getValue(CF_DEFAULT.getBytes(), "rank".getBytes())));
                    System.out.println("status:" + Bytes.toInt(r.getValue(CF_DEFAULT.getBytes(), "status".getBytes())));
                    System.out.println("legal_person_type:" + Bytes.toInt(r.getValue(CF_DEFAULT.getBytes(), "legal_person_type".getBytes())));
                    System.out.println("legal_person_id_type:" + Bytes.toInt(r.getValue(CF_DEFAULT.getBytes(), "legal_person_id_type".getBytes())));
                    System.out.println("bank_account_verify_status:" + Bytes.toInt(r.getValue(CF_DEFAULT.getBytes(), "bank_account_verify_status".getBytes())));
                    System.out.println("platform:" + Bytes.toInt(r.getValue(CF_DEFAULT.getBytes(), "platform".getBytes())));
                    System.out.println("version:" + Bytes.toLong(r.getValue(CF_DEFAULT.getBytes(), "version".getBytes())));
                    System.out.println("extra:" + Bytes.toString(r.getValue(CF_DEFAULT.getBytes(), "extra".getBytes())));
                }


            } finally {
                if (table != null) table.close();
            }


        } catch (IOException e) {

        } finally {

            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }


    }

}
