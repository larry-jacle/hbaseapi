package cn.com.jacle;

import cn.com.jacle.utils.HbaseUtil;
import org.junit.Test;

import java.io.IOException;

/**
 * Description:
 * author:Jacle
 * Date:2018/12/10
 * Time:11:28
 **/
public class utilstest
{
    @Test
    public void testHbaseUtil()
    {
        try
        {
            HbaseUtil.createTable("hbaseApi_test", new String[]{"test1","test2"});
            System.out.println("done");
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void testAddRow()
    {
        try
        {
            HbaseUtil.insertRow("2#", "test_preregion", "preregionColF", "test1", "test1");
            System.out.println(" insert done!");
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void testDelete()
    {
        try
        {
            HbaseUtil.delRow("2", "hbaseApi_test", "test1", "test1");
            System.out.println(" delete done!");
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }


    @Test
    public void getData()
    {
        try
        {
            HbaseUtil.getData("hbaseApi_test", "1", "", "");
            System.out.println(" getData done!");
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void scanData()
    {
        try
        {
            //这种总之是指定了位数，所以一般rowkey的位数是固定的要
            HbaseUtil.scanData("hbaseApi_test", "5#", "5:");
            System.out.println(" scanData done!");
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void preRegionTest()
    {
        try
        {
            //这种总之是指定了位数，所以一般rowkey的位数是固定的要
            HbaseUtil.createTablePreRegion("test_preregion", new String[]{"preregionColF"}, new String[]{"1","2","3"});
            System.out.println(" cratePreRegion done!");
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

}
