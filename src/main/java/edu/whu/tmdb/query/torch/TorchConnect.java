package edu.whu.tmdb.query.torch;

import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.torch.proto.IdEdge;
import edu.whu.tmdb.query.torch.proto.IdEdgeRaw;
import edu.whu.tmdb.query.torch.proto.IdVertex;
import edu.whu.tmdb.query.torch.proto.TrajectoryTimePartial;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import au.edu.rmit.bdm.Test;
import au.edu.rmit.bdm.Torch.base.Torch;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.base.model.Trajectory;
import au.edu.rmit.bdm.Torch.queryEngine.Engine;
import au.edu.rmit.bdm.Torch.queryEngine.model.SearchWindow;
import au.edu.rmit.bdm.Torch.queryEngine.query.QueryResult;
import edu.whu.tmdb.query.excecute.Create;
import edu.whu.tmdb.query.utils.TMDBException;
import edu.whu.tmdb.query.excecute.Insert;
import edu.whu.tmdb.query.excecute.Select;
import edu.whu.tmdb.query.excecute.impl.CreateImpl;
import edu.whu.tmdb.query.excecute.impl.InsertImpl;
import edu.whu.tmdb.query.excecute.impl.SelectImpl;
import edu.whu.tmdb.query.utils.Constants;
import edu.whu.tmdb.query.utils.MemConnect;
import edu.whu.tmdb.query.utils.SelectResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static edu.whu.tmdb.util.FileOperation.getFileNameWithoutExtension;

public class TorchConnect {

    private static Logger logger = LoggerFactory.getLogger(TorchConnect.class);
    Engine engine;
    String baseDir;
    MemConnect memConnect;
    public static TorchConnect torchConnect;

    public TorchConnect(MemConnect memConnect, String baseDir){
        this.baseDir=Constants.TORCH_RES_BASE_DIR+"/"+baseDir;
        this.memConnect=memConnect;
//        this.engine=Engine.getBuilder().baseDir(baseDir).build();
//        this.helper=new TorchSQLiteHelper(this.baseDir+"/Torch/db/"+baseDir+".db");
    }

    public static void init(MemConnect memConnect, String baseDir){
        torchConnect=new TorchConnect(memConnect, baseDir);
    }

    public static TorchConnect getTorchConnect(){
        return torchConnect;
    }

    public void test() throws IOException {
        test("data/res/raw/query.txt");
    }
    public void test(String querySrc) throws IOException {
        List<List<TrajEntry>> queries = Test.read(querySrc);
        QueryResult topK = engine.findTopK(queries.get(0), 3);
        QueryResult result = engine.findOnPath(queries.get(1));
        System.out.println((topK.toJSON(1)));
        System.out.println(result.toJSON(1));
    }

    public void mapMatching(String trajSrc,String osmSrc) {
//        String filePath = Constants.TORCH_RES_BASE_DIR+"/raw/porto_raw_trajectory.txt"; // 替换为实际的文件路径
        String pbfFilePath=osmSrc;
        Test.init(baseDir,trajSrc,pbfFilePath);
        Transaction.getInstance().SaveAll();
    }

    public void initEngine() {
        engine=Engine.getBuilder().baseDir(baseDir).build();
//        System.out.println(1);
    }

    public List<Trajectory<TrajEntry>> rangeQuery(SearchWindow searchWindow){
        QueryResult inRange = engine.findInRange(searchWindow);
        return inRange.resolvedRet;
    }

    public List<Trajectory<TrajEntry>> pathQuery(Trajectory trajectory){
        QueryResult onPath = engine.findOnPath(trajectory);
        return onPath.resolvedRet;
    }

    public List<Trajectory<TrajEntry>> pathQuery(String pathName){
        QueryResult onPath = engine.findOnPath(pathName);
        return onPath.resolvedRet;
    }

    public List<Trajectory<TrajEntry>> strictPathQuery(Trajectory trajectory){
        QueryResult onPath = engine.findOnStrictPath(trajectory);
        return onPath.resolvedRet;
    }

    public List<Trajectory<TrajEntry>> strictPathQuery(String pathName){
        QueryResult onPath = engine.findOnStrictPath(pathName);
        return onPath.resolvedRet;
    }

    public List<Trajectory<TrajEntry>> topkQuery(Trajectory trajectory,int k,String similarityFunction)  {
//        engine=Engine.getBuilder().preferedSimilarityMeasure(similarityFunction).baseDir(baseDir).build();
        QueryResult onPath = engine.findTopK(trajectory,k);
        return onPath.resolvedRet;
    }

    public List<Trajectory<TrajEntry>> topkQuery(Trajectory trajectory,int k){
        QueryResult onPath = engine.findTopK(trajectory,k);
        return onPath.resolvedRet;
    }


    //将traj数据插入tmdb中，原始的轨迹数据
    public void insert(String srcPath){
        BufferedReader reader = null;
        String sql="CREATE CLASS traj (traj_id int,user_id char,traj_name char,traj double[]);";
        Create create=new CreateImpl();
        try {
            create.create(CCJSqlParserUtil.parse(sql));
        }catch (TMDBException e){
            System.out.println(e.getMessage());
        } catch (JSQLParserException e) {
            logger.warn(e.getMessage());
        }
        try {
            // 读取文件路径
            String filePath = srcPath;
            reader = new BufferedReader(new FileReader(filePath));
            String line;
            List<List<TrajEntry>> list=new ArrayList<>();
            // 逐行读取文件内容
            while ((line = reader.readLine()) != null) {
                String[] sa=line.split("\\s+");
                String temp=sa[1];
//                traj=traj.replace("[","").replace("]","").replace(",","|");
                temp=temp.replace("[","").replace("]","");
                String[] split = temp.split(",");

                double[] traj= Arrays.stream(split).mapToDouble(e -> Double.parseDouble(e)).toArray();
                sql="Insert into traj values ("+sa[0]+",-1,'"+getFileNameWithoutExtension(srcPath)+"',Array"+Arrays.toString(traj)+")";
//                sql="Insert into traj values ("+sa[0]+",-1,'"+getFileNameWithoutExtension(srcPath)+"','"+traj+"')";
                Transaction.getInstance().query(sql);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                // 关闭文件读取器
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}


