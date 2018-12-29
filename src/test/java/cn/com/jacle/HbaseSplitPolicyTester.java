package cn.com.jacle;

import cn.com.jacle.utils.HbaseUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

/**
 * Description:
 * author:Jacle
 * Date:2018/12/27
 * Time:14:15
 **/
public class HbaseSplitPolicyTester
{

    @Test
    public void testRegionSplit()
    {
        try
        {
            for(int i=0;i<100000;i++)
            {
                HbaseUtil.insertRow(UUID.randomUUID().toString(), "test_region", "cf1", "test1", "IncreasingToUpperBoundRegionSplitPolicy，0.94.0默认region split策略。根据公式min(r^2*flushSize，maxFileSize)确定split的maxFileSize，其中r为在线region个数，maxFileSize由hbase.hregion.max.filesize指定");
                System.out.println(" insert done!");
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

}
