package edu.whu.tmdb;/*
 * className:${NAME}
 * Package:edu.whu.tmdb
 * Description:
 * @Author: xyl
 * @Create:${DATE} - ${TIME}
 * @Version:v1
 */

import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.utils.SelectResult;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
//        execute("CREATE CLASS id_vertex (name char,age int, salary int);");
//        execute("SELECT * FROM trajectory_vertex WHERE traj_name = 'Torch' LIMIT 100000");
//        execute("SELECT * FROM trajectory_vertex WHERE traj_name = 'Torch'");
//        execute("CREATE CLASS traj (traj_id int,user_id char,traj_name char,traj double[]);");
//        execute("select edges from trajectory_edge_partial where id=1 and traj_name='Torch';");
//        execute("select edges from trajectory_edge_partial where id=1;");
//        execute("select * from traj where traj_name='porto_raw_trajectory';");
//        execute("SELECT * FROM trajectory_edge WHERE traj_name = 'Torch' LIMIT 100000");
//        execute("CREATE CLASS company (name char,age int, salary int);");
//        execute("INSERT INTO company VALUES (aa,20,1000);");
//        execute("INSERT INTO company VALUES (ab,30,Array[1000,2000]);");
//        execute("INSERT INTO company VALUES (ac,40,1000);");
//        execute("create selectdeputy deputy as select * from company limit 1;");
//        execute("select * from traj"+
//                " where traj_name='"+getFileNameWithoutExtension("data/res/raw/porto_raw_trajectory.txt")+"';");
//        execute("INSERT INTO company VALUES (ab,30,Array[-1000,2000]);");
//          testTopkQuery();
//          testPathQuery();
//        testStreamline();
//        insertIntoTrajTable();
//        execute("select * from traj");
//        testMapMatching();
//        testEngine();
//        testTorch3();
    }

    public static void testTopkQuery(){
        Transaction transaction = Transaction.getInstance();
        transaction.initEngine();
        execute("select traj_id,traj\n" +
                "from traj\n" +
                "where traj_name='porto_raw_trajectory' and " +
                "st_similarity(Trajectory(-8.639847,41.159826,-8.640351,41.159871,-8.642196,41.160114,-8.644455,41.160492,-8.646921,41.160951,-8.649999,41.161491,-8.653167,41.162031,-8.656434,41.16258,-8.660178,41.163192,-8.663112,41.163687,-8.666235,41.1642,-8.669169,41.164704,-8.670852,41.165136,-8.670942,41.166576,-8.66961,41.167962,-8.668098,41.168988,-8.66664,41.170005,-8.665767,41.170635,-8.66574,41.170671\n" +
                "                              ), 3);");
    }

    public static void testPathQuery(){
        Transaction transaction = Transaction.getInstance();
        transaction.initEngine();
        execute("select traj_id, traj\n" +
                "from traj\n" +
                "where traj_name='porto_raw_trajectory' and " +
                "st_intersect(Trajectory(-8.639847,41.159826,-8.640351,41.159871,-8.642196,41.160114,-8.644455,41.160492,-8.646921,41.160951,-8.649999,41.161491,-8.653167,41.162031,-8.656434,41.16258,-8.660178,41.163192,-8.663112,41.163687,-8.666235,41.1642,-8.669169,41.164704,-8.670852,41.165136,-8.670942,41.166576,-8.66961,41.167962,-8.668098,41.168988,-8.66664,41.170005,-8.665767,41.170635,-8.66574,41.170671\n" +
                "                             ));");
    }

    public static void testStreamline() throws IOException {
        String base="data/res/raw/";
        Transaction.getInstance().streamLine("Torch",
                base+"porto_raw_trajectory.txt",
                base +"Porto.osm.pbf",
                base+"query.txt");
    }
    public static void insertIntoTrajTable(){
        Transaction transaction = Transaction.getInstance();
        transaction.insertIntoTrajTable();
        transaction.SaveAll();
    }

    public static void testEngine() throws IOException {
        Transaction transaction = Transaction.getInstance();
        transaction.testEngine();
    }



    public static void testMapMatching() {
        Transaction transaction = Transaction.getInstance();
        transaction.testMapMatching();
    }

    public static SelectResult execute(String s)  {
        Transaction transaction = Transaction.getInstance();
        Statement stmt = null;
        SelectResult selectResult = new SelectResult();
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(s.getBytes());
            //使用JSqlparser进行sql语句解析，会根据sql类型生成对应的语法树。
            stmt= CCJSqlParserUtil.parse(byteArrayInputStream);
            selectResult=transaction.query("", -1, stmt);
        }catch (JSQLParserException e) {
            e.printStackTrace();
        }
        if(!stmt.getClass().getSimpleName().toLowerCase().equals("select")){
            transaction.SaveAll();
        }
        return selectResult;
    }
}