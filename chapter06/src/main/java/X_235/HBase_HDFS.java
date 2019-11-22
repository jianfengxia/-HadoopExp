package X_235;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
public class HBase_HDFS {
    //初始化
    private static Connection connection = null;
    private static Admin admin = null;
    //静态代码块
    static {

        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","localhost");
            configuration.set("hbase.rootdir", "hdfs://localhost:8020/hbase");
            configuration.set("hbase.cluster.distributed", "true");
            //创建到 HBase 的连接
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean isTableExist(String tableName) throws IOException {
        //获取配置信息
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        return exists;
    }
    //构造close方法
    public static void close()  {
        if(admin != null);{
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(connection!=null);
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //创建表
    public static void createTable(String tableName,String... cfs) throws IOException {
        if (cfs.length<=0){
            System.out.println("设置列族信息ing");
            return;
        }
        if (isTableExist(tableName)){
            System.out.println(tableName+"已存在");
            return;
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        admin.createTable(hTableDescriptor);
    }
    //插入数据
    public static void insert(String rowKey, String tableName,
                              String[] column1, String[] value1, String[] column2, String[] value2,
                              String[] column3, String[] value3) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
        for (int i = 0; i < columnFamilies.length; i++) {
            String f = columnFamilies[i].getNameAsString();
            if (f.equals("member_id")) {
                for (int j = 0; j < column1.length; j++) {
                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
            if (f.equals("address")) {
                for (int j = 0; j < column2.length; j++) {
                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }
            if (f.equals("info")) {
                for (int j = 0; j < column3.length; j++) {
                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column3[j]), Bytes.toBytes(value3[j]));
                }
            }
        }
        table.put(put);
        table.close();
    }

    //获取表
    public static void get(String tableName,String rowKey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        System.out.println("#################查看"+cf+"列  ing##################");
        get.addFamily(Bytes.toBytes(cf));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(" 行键："+Bytes.toString(CellUtil.cloneRow(cell))+", 列族："+Bytes.toString(CellUtil.cloneFamily(cell))+
                    ", 列名："+Bytes.toString(CellUtil.cloneQualifier(cell))+
                    ", 值："+Bytes.toString(CellUtil.cloneValue(cell)));

        }

    }
    //扫描表
    public static void scan(String tableName,String rowKey,String cf,String cn) throws IOException {
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes(rowKey));
        scan.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println( " 行键："+Bytes.toString(CellUtil.cloneRow(cell))+", 列族："+Bytes.toString(CellUtil.cloneFamily(cell))+
                        ", 列名："+Bytes.toString(CellUtil.cloneQualifier(cell))+
                        ", 值："+Bytes.toString(CellUtil.cloneValue(cell)));


            }

        }

        table.close();

    }
    //测试代码，主方法
    public static void main(String[] args) throws IOException {

        System.out.println("##########################################################");
        System.out.println("数学17~2“&”17124080235");
        System.out.println("”17124080235");
        System.out.println("##########################################################");
        System.out.println("创建emp17124080235");
        createTable("emp17124080235","member_id","address","info");
        System.out.println("##########################################################");
        System.out.println("查看表是否存在");
        System.out.println(isTableExist("emp17124080235"));

          String [] column1 = {"id"};String [] column2 = {"city","country"};
          String [] column3 ={"age","brithday","industry"};
          String [] value1 = {"235"};String [] value2 = {"New666","China"} ;
          String [] value3 = {"20","2019-66-66","student"};
          insert("17124080235","emp17124080235",column1,value1,column2,value2,column3,value3);

        get("emp17124080235","17124080235","info","city");

        System.out.println("#############toolZhuangzixuan@扫描info:brithay列###############");
        scan("emp17124080235","17124080235","info","brithday");
        System.out.println("#################挥手一分钟#####################");
        close();
    }
}

