package com.hbase.test;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HbaseInsert1 {
    public static Log log=LogFactory.getLog(HbaseInsert1.class);
    public static String al_TableName="employees";//表名
    public static String columnFamily="student_name";//列族
    public static String rowFamily="h2";//行键
    static Configuration conf=null;
//    
    static{
//        Configuration confi=new HBaseConfiguration().create();
//        conf=confi;
//         //指定zookeeper
//        conf.set("hbase.zookeeper.quorum", "node4");
//        System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.4");//设置hadoop的安装目录，如果在eclipse已经设置过可以不用设置
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "62f169f8d9d1");// 使用eclipse时必须添加这个，否则无法定位master需要配置hosts
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.master", "202.204.62.145:16010");
//        conf.set("hbase.root.dir", "hdfs://202.204.62.145:9000/hbase");//这个根据hbase-site.xml的配置文件写
    }
    
    public static void main(String[] args) throws IOException {
    	testCreateTable();
    }
    
    /**
     * 创建表
     * HBaseAdmin 
     * HTableDescriptor
     */
    @Test
    public static void testCreateTable(){
        
        HBaseAdmin admin=null;
        HTableDescriptor htd=null;
        try {
             admin=new HBaseAdmin(conf);
            //判断要创建的表是否已经存在
            if(admin.tableExists(al_TableName)){
                System.out.println(al_TableName+"表已经存在");
                return;
            }else{
                htd=new HTableDescriptor(al_TableName.getBytes());    
                //设置列族
                htd.addFamily(new HColumnDescriptor(columnFamily));
                admin.createTable(htd);
                System.out.println(al_TableName+"表创建成功！！！");
            }
            
//            htd=new HTableDescriptor(al_TableName.getBytes());    
//            //设置列族
//            htd.addFamily(new HColumnDescriptor(columnFamily));
//            admin.createTable(htd);
//            System.out.println(al_TableName+"表创建成功！！！");
            
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 删除表
     */
    @Test
    public static void testDeletTable(){
        HBaseAdmin admin=null;
        try {
            admin=new HBaseAdmin(conf);
            if(admin.tableExists(al_TableName)){
                System.out.println("table is exits");
                //如果表存在，则注为失效状态
                admin.disableTable(al_TableName);//删除表
                admin.deleteTable(al_TableName);
                System.out.println("删除成功");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
