package edu.whu.tmdb;/*
 * className:${NAME}
 * Package:edu.whu.tmdb
 * Description:
 * @Author: xyl
 * @Create:${DATE} - ${TIME}
 * @Version:v1
 */

import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.torch.TorchConnect;
import edu.whu.tmdb.query.utils.KryoSerialization;
import edu.whu.tmdb.query.utils.SelectResult;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.utils.K;
import edu.whu.tmdb.storage.utils.V;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.management.MemoryManagerMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
//        execute("CREATE CLASS t (name char,age int, salary int);");
//        execute("select * from t where a=1 and b=2 or c=3 or d=4;");
//        testTopkQuery("Porto");
//        testPathQuery("Porto");
//        testRangeQueryRadius("Porto");
        testRangeQuerySquare("Porto");
//        testMapMatching();
//        test();
    }

    public static void test(){
        String ks="Porto/gridCard";
        V search = MemManager.getInstance().search(new K(ks));
        HashMap<Integer,Integer> gridCard = (HashMap<Integer, Integer>) KryoSerialization.deserializeFromString(search.valueString);
        System.out.println(gridCard.size());
//        HashMap<Integer,Integer> o = (HashMap<Integer, Integer>) KryoSerialization.deserializeFromString(s);
//        System.out.println(o.toString());
//        execute("CREATE CLASS t (name char,age int, salary int);");
//        execute("insert into t values ('aa',1,1),('aa',2,1),('bb',1,1);");
////        Transaction.getInstance().SaveAll();
//        execute("select name,Max(age) from t group by name");
    }

    public static void testMapMatching() throws IOException {
        Transaction transaction=Transaction.getInstance();
        transaction.mapMatching("Porto",
                "data/res/raw/porto_raw_trajectory.txt",
                "data/res/raw/Porto.osm.pbf");
    }

    public static void testTopkQuery(String baseDir){
        Transaction.getInstance().initEngine(baseDir);
        execute("select traj_id,traj\n" +
                "from traj\n" +
                "where st_similarity(Trajectory(" +
                "-8.643807,41.168979,-8.642529,41.170113,-8.642133,41.171202,-8.64324,41.172723,-8.644788,41.173947,-8.646534,41.175558,-8.648829,41.177367,-8.649828,41.17824,-8.647911,41.1786" +
                "), 10);");
    }

    public static void testPathQuery(String baseDir){
        Transaction.getInstance().initEngine(baseDir);
        execute("select traj_id, traj\n" +
                "from traj\n" +
                "where traj_name='porto_raw_trajectory' and " +
                "st_intersect(Trajectory(-8.639847,41.159826,-8.640351,41.159871,-8.642196,41.160114,-8.644455,41.160492,-8.646921,41.160951,-8.649999,41.161491,-8.653167,41.162031,-8.656434,41.16258,-8.660178,41.163192,-8.663112,41.163687,-8.666235,41.1642,-8.669169,41.164704,-8.670852,41.165136,-8.670942,41.166576,-8.66961,41.167962,-8.668098,41.168988,-8.66664,41.170005,-8.665767,41.170635,-8.66574,41.170671\n" +
                "                             ));");
    }

    public static void testRangeQuerySquare(String baseDir){
        Transaction.getInstance().initEngine(baseDir);
        execute("select traj_id, traj\n" +
                "from traj\n" +
                "where traj_name='porto_raw_trajectory' and " +
                "st_within(SearchWindow(Coordiante(-8.640717,41.160375),Coordiante(-8.638977,41.159277)));");
    }

    public static void testRangeQueryRadius(String baseDir){
        Transaction.getInstance().initEngine(baseDir);
        execute("select traj_id, traj\n" +
                "from traj\n" +
                "where traj_name='porto_raw_trajectory' and " +
                "st_within(SearchWindow(Coordiante(-8.639847,41.159826),50));");
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

//    public static void main(String[] args) {
//        String inputFilePath = "data/res/raw/porto_raw_trajectory.txt"; // 替换为输入文件的路径
//        String outputFilePath = "data/res/raw/sql.txt"; // 替换为输出文件的路径
//
//        try {
//            List<String> sqlStatements = new ArrayList<>();
//            List<String> lines = Files.readAllLines(Paths.get(inputFilePath));
//
//            for (String line : lines) {
//                String[] parts = line.split("\\s+", 2);
//                String id = parts[0];
////                String formattedTrajectory = parts[1].trim().replaceAll("\\[|\\]", "");
////
////                // 替换所有内部的多余空格为单个空格
////                formattedTrajectory = formattedTrajectory.replaceAll("\\s+", " ");
////
////                // 将空格分隔的坐标对转换为WKT格式（使用逗号加空格分隔）
////                formattedTrajectory = formattedTrajectory.replaceAll("([0-9\\.]+)\\s+([0-9\\.]+)", "$1 $2, ");
////
////                // 移除最后一个逗号
////                formattedTrajectory = formattedTrajectory.replaceAll(", $", "");
//                String formattedTrajectory = parts[1].trim().replaceAll("\\[|\\]", "");
//
//                // 正确处理逗号，确保只在坐标对之间添加逗号，并去除坐标内部的逗号
//                formattedTrajectory = formattedTrajectory.replaceAll(",\\s*", " "); // 去掉坐标间的逗号，用空格代替
//                formattedTrajectory = formattedTrajectory.trim().replaceAll("\\s+", ","); // 将坐标对间的空格替换为逗号加空格
//                String[] split = formattedTrajectory.split(",");
//                StringBuilder sb=new StringBuilder("");
//                for (int i = 0; i < split.length-2; i+=2) {
//                    sb.append(split[i]).append(" ").append(split[i+1]).append(",");
//                }
//                sb.append(split[split.length-2]).append(" ").append(split[split.length-1]);
//                // 创建WKT LINESTRING
//
//                String sql = String.format(
//                        "INSERT INTO Trajectories (id, path) VALUES (%s, GeomFromText('LINESTRING(%s)', 4326));",
//                        id, sb.toString());
//                sqlStatements.add(sql);
//            }
//
//            Path outputPath = Paths.get(outputFilePath);
//            Files.write(outputPath, sqlStatements, StandardCharsets.UTF_8);
//
//            System.out.println("SQL statements have been written to: " + outputFilePath);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }


}