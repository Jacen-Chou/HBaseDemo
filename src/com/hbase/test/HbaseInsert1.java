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
    public static String al_TableName="employees";//����
    public static String columnFamily="student_name";//����
    public static String rowFamily="h2";//�м�
    static Configuration conf=null;
//    
    static{
//        Configuration confi=new HBaseConfiguration().create();
//        conf=confi;
//         //ָ��zookeeper
//        conf.set("hbase.zookeeper.quorum", "node4");
//        System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.4");//����hadoop�İ�װĿ¼�������eclipse�Ѿ����ù����Բ�������
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "62f169f8d9d1");// ʹ��eclipseʱ�����������������޷���λmaster��Ҫ����hosts
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.master", "202.204.62.145:16010");
//        conf.set("hbase.root.dir", "hdfs://202.204.62.145:9000/hbase");//�������hbase-site.xml�������ļ�д
    }
    
    public static void main(String[] args) throws IOException {
    	testCreateTable();
    }
    
    /**
     * ������
     * HBaseAdmin 
     * HTableDescriptor
     */
    @Test
    public static void testCreateTable(){
        
        HBaseAdmin admin=null;
        HTableDescriptor htd=null;
        try {
             admin=new HBaseAdmin(conf);
            //�ж�Ҫ�����ı��Ƿ��Ѿ�����
            if(admin.tableExists(al_TableName)){
                System.out.println(al_TableName+"���Ѿ�����");
                return;
            }else{
                htd=new HTableDescriptor(al_TableName.getBytes());    
                //��������
                htd.addFamily(new HColumnDescriptor(columnFamily));
                admin.createTable(htd);
                System.out.println(al_TableName+"�����ɹ�������");
            }
            
//            htd=new HTableDescriptor(al_TableName.getBytes());    
//            //��������
//            htd.addFamily(new HColumnDescriptor(columnFamily));
//            admin.createTable(htd);
//            System.out.println(al_TableName+"�����ɹ�������");
            
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * ɾ����
     */
    @Test
    public static void testDeletTable(){
        HBaseAdmin admin=null;
        try {
            admin=new HBaseAdmin(conf);
            if(admin.tableExists(al_TableName)){
                System.out.println("table is exits");
                //�������ڣ���עΪʧЧ״̬
                admin.disableTable(al_TableName);//ɾ����
                admin.deleteTable(al_TableName);
                System.out.println("ɾ���ɹ�");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
