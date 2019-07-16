package io.svectors.hbase.check;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.Test;

import java.io.IOException;

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


                Scan scan = new Scan();
                scan.setFilter(new PageFilter(50));
                SingleColumnValueFilter scvf = new SingleColumnValueFilter(CF_DEFAULT.getBytes(), "extra".getBytes(), CompareFilter.CompareOp.NOT_EQUAL, "0".getBytes());
                scvf.setFilterIfMissing(true);
                scan.setFilter(scvf);

                ResultScanner scanner = table.getScanner(scan);
                for (Result result : scanner) {
                    System.out.println(new String(result.getRow()));
                    byte[] b = result.getValue(CF_DEFAULT.getBytes(), "extra".getBytes());
                    System.out.println(new String(b, "utf8"));
                }

//                Get get = new Get("b9fc12fb29c6-b108-5e11-7dce-caa4ee8c".getBytes());
//                Result r = table.get(get);
//                if (r.isEmpty()) {
//                    System.out.println("result is null");
//                } else {
//                    System.out.println(new String(r.getRow()));
//                    System.out.println(r.getValue(CF_DEFAULT.getBytes(), "extra".getBytes()));
//                }


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
