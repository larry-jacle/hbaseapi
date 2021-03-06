package cn.com.jacle.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Description:
 * author:Jacle
 * Date:2018/12/25
 * Time:14:04
 **/
public class SelfCoProcessor  extends BaseRegionObserver
{
    private static final byte[] TABLE_NAME = Bytes.toBytes("index_name_users");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("personalDet");
    private static final byte[] COLUMN = Bytes.toBytes("name");

    private Configuration configuration = HBaseConfiguration.create();

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
            throws IOException {

        HTable indexTable = new HTable(configuration, TABLE_NAME);

        List<Cell> cells = put.get(COLUMN_FAMILY, COLUMN);
        Iterator<Cell> cellIterator = cells.iterator();
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            Put indexPut = new Put(CellUtil.cloneValue(cell));
            indexPut.add(COLUMN_FAMILY, COLUMN, CellUtil.cloneRow(cell));
            indexTable.put(indexPut);
        }
    }
}
