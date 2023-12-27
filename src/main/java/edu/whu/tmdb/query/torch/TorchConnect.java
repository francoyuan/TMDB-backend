package edu.whu.tmdb.query.torch;

import edu.whu.tmdb.query.Transaction;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

import java.io.*;
import java.util.*;

import au.edu.rmit.bdm.Test;
import au.edu.rmit.bdm.Torch.base.Torch;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.base.model.Trajectory;
import au.edu.rmit.bdm.Torch.queryEngine.Engine;
import au.edu.rmit.bdm.Torch.queryEngine.model.SearchWindow;
import au.edu.rmit.bdm.Torch.queryEngine.query.QueryResult;
import edu.whu.tmdb.query.excecute.Create;
import edu.whu.tmdb.query.utils.TMDBException;
import edu.whu.tmdb.query.excecute.impl.CreateImpl;
import edu.whu.tmdb.query.utils.Constants;
import edu.whu.tmdb.query.utils.MemConnect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static edu.whu.tmdb.util.FileOperation.getFileNameWithoutExtension;

public class TorchConnect {

    private static Logger logger = LoggerFactory.getLogger(TorchConnect.class);
    Engine engine;
    String baseDir;
    MemConnect memConnect;
    public static TorchConnect torchConnect;

    private boolean init = false;

    public TorchConnect(MemConnect memConnect, String baseDir){
        this.baseDir=Constants.TORCH_RES_BASE_DIR+"/"+baseDir;
        this.memConnect=memConnect;
//        this.engine=Engine.getBuilder().baseDir(baseDir).build();
//        this.helper=new TorchSQLiteHelper(this.baseDir+"/Torch/db/"+baseDir+".db");
    }

    public void updateTrajMap(ArrayList<String> trajIds){
        Map<String, String[]> trajs = this.engine.getPool().getResolver().getTrajectoryPool().getMemPool();
        Map<String, String[]> res=new HashMap<>();
        for (String trajId :
                trajIds) {
            res.put(trajId,trajs.get(trajId));
        }
        this.engine.getPool().getResolver().getTrajectoryPool().setMemPool(res);
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
        if(init) return;
        engine=Engine.getBuilder().baseDir(baseDir).build();
        init=true;
    }

    public void initEngine(String queryType){
        if(init) return;
        engine=Engine.getBuilder().baseDir(baseDir).addQuery(queryType).build();
        init=true;
    }

    public void setResolveAll(Boolean resolveAll){

    }

    public List<Trajectory<TrajEntry>> rangeQuery(SearchWindow searchWindow){
        QueryResult inRange = engine.findInRange(searchWindow);
        return inRange.resolvedRet;
    }

    public int[] rangeQueryIds(SearchWindow searchWindow){
        QueryResult inRange = engine.findInRange(searchWindow);
        return inRange.idArray;
    }

    public int[] pathQuery(Trajectory trajectory){
        QueryResult onPath = engine.findOnPath(trajectory);
        return onPath.idArray;
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
        engine=Engine.getBuilder().preferedSimilarityMeasure(similarityFunction).preferedIndex(Torch.Index.LEVI).baseDir(baseDir).build();
        long start=System.currentTimeMillis();
        QueryResult onPath = engine.findTopK(trajectory,k);
        long end=System.currentTimeMillis();
        long duration = (end - start) / 1_000_000; // 转换为毫秒
        System.out.println("TopK 查询消耗： "+duration);
        return onPath.resolvedRet;
    }

    public List<Trajectory<TrajEntry>> topkQuery(Trajectory trajectory,int k){
        QueryResult onPath = this.engine.findTopK(trajectory,k);
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
            int limit=10000;
            int i=0;
            // 逐行读取文件内容
            while ((line = reader.readLine()) != null) {
                String[] sa=line.split("\\s+");
                String temp=sa[1];
//                traj=traj.replace("[","").replace("]","").replace(",","|");
                temp=temp.replace("[","").replace("]","");
                String[] split = temp.split(",");
                if(split.length>200){
                    continue;
                }
                double[] traj= Arrays.stream(split).filter(e-> e.length()!=0).mapToDouble(e -> Double.parseDouble(e)).toArray();
                sql="Insert into traj values ("+sa[0]+",-1,'"+getFileNameWithoutExtension(srcPath)+"',Array"+Arrays.toString(traj)+")";
//                sql="Insert into traj values ("+sa[0]+",-1,'"+getFileNameWithoutExtension(srcPath)+"','"+traj+"')";
                Transaction.getInstance().query(sql);
                if(i++>limit){
                    break;
                }
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


