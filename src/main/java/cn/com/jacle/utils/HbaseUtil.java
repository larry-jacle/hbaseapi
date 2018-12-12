package cn.com.jacle.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Description:Hbase工具类
 * author:Jacle
 * Date:2018/12/6
 * Time:13:09
 * 测试工具本地的host文件必须包含hbase的maste、region的hostname映射
 **/
public class HbaseUtil
{
    private static Configuration config;
    private static Connection connection;
    private static Admin admin;
    private static ExecutorService threadPool= Executors.newFixedThreadPool(4);


    //静态资源初始化
    public static void init()
    {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "s203");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        try
        {
            connection = ConnectionFactory.createConnection(config,threadPool);
            admin = connection.getAdmin();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
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


    /**
     * 创建hbase的表格
     *
     * @param tablename
     * @param cols
     * @throws IOException
     */
    public static void createTable(String tablename, String[] cols) throws IOException
    {
        init();

        TableName tname = TableName.valueOf(tablename);

        if (admin.tableExists(tname))
        {
            System.out.println(tablename + "存在");
        } else
        {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tname);
            for (String col : cols)
            {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }

            admin.createTable(hTableDescriptor);
        }
        closeResources();
    }


    /**
     * splitkeys是保存字节数组的数组
     * @param keys
     * @return
     */
    private  static byte[][] getSplitKeys(String[] keys)
    {
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

    /**
     * 创建表格并且预分区
     * @param tablename
     * @param cols
     * @throws IOException
     */
    public static void createTablePreRegion(String tablename, String[] cols,String[] keys) throws IOException
    {
        init();

        TableName tname = TableName.valueOf(tablename);

        if (admin.tableExists(tname))
        {
            System.out.println(tablename + "存在");
        } else
        {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tname);
            for (String col : cols)
            {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }

            byte[][]  splitKeys=getSplitKeys(keys);
            admin.createTable(hTableDescriptor,splitKeys);
        }
        closeResources();
    }

    /**
     * 删除表格
     *
     * @param tname
     * @throws IOException
     */
    public static void delTable(String tname) throws IOException
    {
        init();

        TableName tableName = TableName.valueOf(tname);
        if (admin.tableExists(tableName))
        {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        closeResources();

    }


    /**
     * 插入单行数据
     *
     * @param rowkey
     * @param tname
     * @param colfamily
     * @param col
     * @param val
     * @throws IOException
     */
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
     * 删除指定的行
     *
     * @param rowkey
     * @param tname
     * @param colfamily
     * @param col
     * @throws IOException
     */
    public static void delRow(String rowkey, String tname, String colfamily, String col) throws IOException
    {
        init();

        Table t = connection.getTable(TableName.valueOf(tname));
        //rowkey保存的是二进制字节码
        Delete delete = new Delete(Bytes.toBytes(rowkey));

//        delete.addFamily(Bytes.toBytes(colfamily));
//        delete.addColumn(Bytes.toBytes(colfamily), Bytes.toBytes(col));

        List<Delete> dels = new ArrayList<Delete>();
        dels.add(delete);

        //批量删除
        t.delete(dels);

        t.close();
        closeResources();
    }


    //根据rowkey查找数据
    public static void getData(String tableName, String rowkey, String colFamily, String col) throws IOException
    {
        long startMillis = System.currentTimeMillis();
        init();
        long point1Millis = System.currentTimeMillis();

        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        //获取指定列族数据
        //get.addFamily(Bytes.toBytes(colFamily));
        //获取指定列数据
        //get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        Result result = table.get(get);

        showCell(result);
        table.close();
        closeResources();
        long point2Millis = System.currentTimeMillis();

        System.out.println(point1Millis - startMillis);
        System.out.println(point2Millis - point1Millis);
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

    //批量查找数据
    public static void scanData(String tableName, String startRow, String stopRow) throws IOException
    {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();

        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));
        scan.setBatch(1000);

        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner)
        {
            showCell(result);
        }
        table.close();
        closeResources();
    }


    public static void main(String[] args) throws  IOException
    {
        init();
        long start=System.currentTimeMillis();

        ArrayList<Put>  puts=new ArrayList<Put>();
        Table t=null;
        for(int i=10000;i<200000;i++)
        {
           t= connection.getTable(TableName.valueOf("hbaseApi_test"));
            //rowkey保存的是二进制字节码
            Put put=new Put(Bytes.toBytes("hello"+i));
            put.addColumn(Bytes.toBytes("test1"), Bytes.toBytes("test1"), Bytes.toBytes("jijiajia 有一点需要说明，因为是线上服务，所以在修改压缩格式后，至comact结束，中间有入库操作，大概有一周到两周的数据入库。也就是说，实际值要比40.055%小一些，但是应该影响不太大。"));

            puts.add(put);

            if(i%500==0)
            {
                //批量添加
                t.put(puts);
                t.close();
                puts.clear();
            }

        }

        if(puts.size()>0)
        {
            t.put(puts);
            t.close();
        }

        System.out.println(System.currentTimeMillis()-start);
        closeResources();
    }

}
