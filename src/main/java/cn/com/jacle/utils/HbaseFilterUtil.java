package cn.com.jacle.utils;

import com.yammer.metrics.stats.EWMA;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Description:
 * author:Jacle
 * Date:2018/12/11
 * Time:12:18
 * 测试过滤器
 **/
public class HbaseFilterUtil
{

    private static Configuration config;
    private static Connection connection;
    private static Admin admin;
    private static ExecutorService threadPool = Executors.newFixedThreadPool(10);

    //静态资源初始化
    public static void init()
    {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "s203");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        try
        {
            connection = ConnectionFactory.createConnection(config, threadPool);
            admin = connection.getAdmin();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }


    public static void insertRow(String rowkey, String tname, String colfamily, String col, String val) throws IOException
    {
        init();

        Table t = connection.getTable(TableName.valueOf(tname));
        //rowkey保存的是二进制字节码
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colfamily), Bytes.toBytes(col), Bytes.toBytes(val));

        ArrayList<Put> puts = new ArrayList<Put>();
        puts.add(put);

        //批量添加
        t.put(puts);
        t.close();
        closeResources();
    }


    /**
     * 关闭连接资源
     */
    public static void closeResources()
    {
        try
        {
            if (admin != null)
            {
                admin.close();
            }

            if (connection != null)
            {
                connection.close();
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }

    }

    public static void scanData(String tableName, String startRow, String stopRow) throws IOException
    {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();


//        scan.addColumn(Bytes.toBytes("test2"),Bytes.toBytes("test2"));

        //rowfilter是从来对rowkey进行过滤的
        //二进制比较器
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("hello10009")));
        //正则比较器，相当于like,开头要加点号
        Filter regFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".*0009"));
        //字符串比较器
        Filter substringFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("9"));
        Filter prefixFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("1000")));
        //prefixfilter
        Filter pFilter = new PrefixFilter(Bytes.toBytes("hello10"));
        Filter keyOnlyFilter = new KeyOnlyFilter(true);
        //只显示test1的
        Filter columnFilter = new ColumnPrefixFilter(Bytes.toBytes("test1"));
        List<Long> timestamps = new ArrayList<Long>();
        timestamps.add(1544508947362l);

        Filter timeFilter = new TimestampsFilter(timestamps);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(timeFilter));
        //列族过滤器
        Filter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("test1")));

        scan.setFilter(familyFilter);

        //scan根据rowkey查找就是通过设置start和stoprow的方式
//        scan.setStartRow(Bytes.toBytes(startRow));
//        scan.setStopRow(Bytes.toBytes(stopRow));
        scan.setBatch(1000);

        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner)
        {
            showCell(result);
        }
        table.close();
        closeResources();
    }

    //格式化输出
    public static void showCell(Result result)
    {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells)
        {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }

    private byte[][] getSplitKeys()
    {
        String[] keys = new String[]{"10|", "20|", "30|", "40|", "50|",
                "60|", "70|", "80|", "90|"};
        byte[][] splitKeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keys.length; i++)
        {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i = 0;
        while (rowKeyIter.hasNext())
        {
            byte[] tempRow = rowKeyIter.next();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }

    public static void main(String[] args)
    {
//            scanData("hbaseApi_test","","");
            /*for (int i = 0; i < 20000; i++)
            {
                insertRow("multi" + i, "hbaseApi_test", "test1", "test1", "test" + i);
            }*/

            System.out.println(new HbaseFilterUtil().getSplitKeys());

    }

}
