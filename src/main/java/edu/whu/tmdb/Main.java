package edu.whu.tmdb;/*
 * className:${NAME}
 * Package:edu.whu.tmdb
 * Description:
 * @Author: xyl
 * @Create:${DATE} - ${TIME}
 * @Version:v1
 */

import au.edu.rmit.bdm.Test;
import au.edu.rmit.bdm.Torch.base.Torch;
import com.alibaba.fastjson2.support.csv.CSVReader;
import edu.whu.tmdb.gtfs.Gtfs2DB;
import edu.whu.tmdb.gtfs.TrajectorySimplifier;
import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.torch.TorchConnect;
import edu.whu.tmdb.query.utils.Helper;
import edu.whu.tmdb.query.utils.KryoSerialization;
import edu.whu.tmdb.query.utils.SelectResult;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.utils.K;
import edu.whu.tmdb.storage.utils.V;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.lang.management.MemoryManagerMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Main {
    public static void main(String[] args) throws IOException {
//        execute("CREATE CLASS t (name char,age int, salary int);");
//        execute("create class porto (id int, geom linestring);");
//        execute("select * from traj");
//        testTopkQuery("Porto");
//        testPathQuery("Porto");
//        testRangeQueryRadius("Porto");
        testRangeQuerySquare("Gtfs");
//        getResult();
//        gtfsMapMatching();
//        getMap();
//        TrajectorySimplifier.simplify();
    }



    public static void test(){
        String ks="Porto/gridCard";
        V search = MemManager.getInstance().search(new K(ks));
        HashMap<Integer,Integer> gridCard = (HashMap<Integer, Integer>) KryoSerialization.deserializeFromString(search.valueString);
        System.out.println(gridCard.size());
    }

    public static void portoMapMatching() throws IOException {
        Transaction transaction=Transaction.getInstance();
        transaction.mapMatching("Porto",
                "data/Porto.txt",
                "data/res/raw/Porto.osm.pbf");
    }

    public static void gtfsMapMatching() throws IOException {
        Transaction transaction=Transaction.getInstance();
        transaction.mapMatching("Gtfs",
                "data/Gtfs_simplified.txt",
                "data/nyc/NewYork.osm.pbf");
    }

    public static void testTopkQuery(String baseDir){
//        Transaction.getInstance().initEngine(baseDir, Torch.QueryType.TopK);
        Transaction.getInstance().initEngine(baseDir);
        execute("select traj_id,traj\n" +
                "from traj\n" +
                "where st_similarity(Trajectory(" +
                "-8.643807,41.168979,-8.642529,41.170113,-8.642133,41.171202,-8.64324,41.172723,-8.644788,41.173947,-8.646534,41.175558,-8.648829,41.177367,-8.649828,41.17824,-8.647911,41.1786" +
                "), 10);");
    }

    public static void testPathQuery(String baseDir){
        Transaction.getInstance().initEngine(baseDir);
        SelectResult execute = execute("select traj_id, traj\n" +
                "from traj\n" +
                "where traj_name='Porto' and " +
                "st_intersect(Trajectory(-8.639847,41.159826,-8.640351,41.159871,-8.642196,41.160114,-8.644455,41.160492,-8.646921,41.160951,-8.649999,41.161491,-8.653167,41.162031,-8.656434,41.16258,-8.660178,41.163192,-8.663112,41.163687,-8.666235,41.1642,-8.669169,41.164704,-8.670852,41.165136,-8.670942,41.166576,-8.66961,41.167962,-8.668098,41.168988,-8.66664,41.170005,-8.665767,41.170635,-8.66574,41.170671\n" +
                "                             ));");
    }

    public static void testRangeQuerySquare(String baseDir){
        Transaction.getInstance().initEngine(baseDir);
//        Transaction.getInstance().initEngine(baseDir,Torch.QueryType.RangeQ);

        execute("select traj_id, traj\n" +
                "from traj\n" +
                "where traj_name='Gtfs_simplified' and " +
                "st_within(SearchWindow(Coordiante(40.73430659479621, -73.98939664060399),Coordiante(40.713433517779464, -73.93999311894093)));");
    }

    public static void testRangeQueryRadius(String baseDir){
        Transaction.getInstance().initEngine(baseDir);
        execute("select traj_id, traj\n" +
                "from traj\n" +
                "where traj_name='Porto' and " +
                "st_within(SearchWindow(Coordiante(-8.639847,41.159826),50))");
    }

    public static void testQuery(String baseDir){
        Transaction.getInstance().initEngine(baseDir);
        execute("select traj_id, traj\n" +
                "from traj\n" +
                "where traj_name='Porto' and " +
                "st_within(SearchWindow(Coordiante(-8.639847,41.159826),50)) and " +
                "st_intersect(Trajectory(-8.639847,41.159826,-8.640351,41.159871,-8.642196,41.160114,-8.644455,41.160492,-8.646921,41.160951,-8.649999,41.161491,-8.653167,41.162031,-8.656434,41.16258,-8.660178,41.163192,-8.663112,41.163687,-8.666235,41.1642,-8.669169,41.164704,-8.670852,41.165136,-8.670942,41.166576,-8.66961,41.167962,-8.668098,41.168988,-8.66664,41.170005,-8.665767,41.170635,-8.66574,41.170671" +
                "));");
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

    public static void writeSql() {
        String inputFilePath = "data/Porto.txt"; // 替换为输入文件的路径
        String outputFilePath = "data/sql.txt"; // 替换为输出文件的路径

        try {
            List<String> sqlStatements = new ArrayList<>();
            List<String> lines = Files.readAllLines(Paths.get(inputFilePath));

            for (String line : lines) {
                String[] parts = line.split("\\s+", 2);
                String id = parts[0];
//                String formattedTrajectory = parts[1].trim().replaceAll("\\[|\\]", "");
//
//                // 替换所有内部的多余空格为单个空格
//                formattedTrajectory = formattedTrajectory.replaceAll("\\s+", " ");
//
//                // 将空格分隔的坐标对转换为WKT格式（使用逗号加空格分隔）
//                formattedTrajectory = formattedTrajectory.replaceAll("([0-9\\.]+)\\s+([0-9\\.]+)", "$1 $2, ");
//
//                // 移除最后一个逗号
//                formattedTrajectory = formattedTrajectory.replaceAll(", $", "");
                String formattedTrajectory = parts[1].trim().replaceAll("\\[|\\]", "");

                // 正确处理逗号，确保只在坐标对之间添加逗号，并去除坐标内部的逗号
                formattedTrajectory = formattedTrajectory.replaceAll(",\\s*", " "); // 去掉坐标间的逗号，用空格代替
                formattedTrajectory = formattedTrajectory.trim().replaceAll("\\s+", ","); // 将坐标对间的空格替换为逗号加空格
                String[] split = formattedTrajectory.split(",");
                StringBuilder sb=new StringBuilder("");
                for (int i = 0; i < split.length-2; i+=2) {
                    sb.append(split[i]).append(" ").append(split[i+1]).append(",");
                }
                if(split.length<2) continue;
                sb.append(split[split.length-2]).append(" ").append(split[split.length-1]);
                // 创建WKT LINESTRING

                String sql = String.format(
                        "INSERT INTO porto (id, geom) VALUES (%s, GeomFromText('LINESTRING(%s)', 4326));",
                        id, sb.toString());
                sqlStatements.add(sql);
            }

            Path outputPath = Paths.get(outputFilePath);
            Files.write(outputPath, sqlStatements, StandardCharsets.UTF_8);

            System.out.println("SQL statements have been written to: " + outputFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties(){
        Properties prop = new Properties();
        try (InputStream input = new FileInputStream("config.properties")) {

            // 加载属性文件
            prop.load(input);
        } catch (IOException io) {
            io.printStackTrace();
        }
        return prop;
    }

    public static void readAndWrite(){
        String csvFile = "/Users/woshi/Downloads/train(1).csv"; // CSV 文件路径
        String txtFile = "data/Porto.txt"; // 输出 TXT 文件路径
        String line = "";
        String csvSeparator = ","; // CSV 文件的分隔符
        int lineNumber=0;
        try (Reader reader = new FileReader(csvFile);
             FileWriter writer = new FileWriter(txtFile);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim())) {
            for (CSVRecord csvRecord : csvParser) {
                String polyline = csvRecord.get("POLYLINE");
                writer.write(lineNumber+" "+polyline + "\n");
                if(lineNumber++>30000){
                    break;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //使用gtfs生成database
    public static void getResult() throws IOException {
        String gtfsPath = "data/gtfs/gtfs.zip"; // 替换为输入文件的路径
        String databasePath = "data/gtfs/gtfs.db"; // 替换为输出文件的路径
        Gtfs2DB.buildDB(gtfsPath,databasePath);
    }

    public static void getMap(){
        String valueString = MemManager.getInstance().search(new K("Qtfs/idMap")).valueString;
        HashMap<Integer, String> integerStringHashMap = (HashMap<Integer, String>) KryoSerialization.deserializeFromString(valueString);
        System.out.println(integerStringHashMap.toString());
    }

}