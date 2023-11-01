package edu.whu.tmdb.query;



import edu.whu.tmdb.query.excecute.impl.*;
import edu.whu.tmdb.query.torch.TorchConnect;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import edu.whu.tmdb.Log.LogManager;
import edu.whu.tmdb.query.excecute.Create;
import edu.whu.tmdb.query.excecute.CreateDeputyClass;
import edu.whu.tmdb.query.excecute.Delete;
import edu.whu.tmdb.query.excecute.Drop;
import edu.whu.tmdb.query.utils.TMDBException;
import edu.whu.tmdb.query.excecute.Insert;
import edu.whu.tmdb.query.excecute.Select;
import edu.whu.tmdb.query.excecute.Update;
import edu.whu.tmdb.query.utils.MemConnect;
import edu.whu.tmdb.query.utils.SelectResult;
import edu.whu.tmdb.storage.level.LevelManager;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Transaction {

    private static Logger logger = LoggerFactory.getLogger(Transaction.class);
    public MemManager mem;
    public LevelManager levelManager;
    public LogManager log;

    private MemConnect memConnect;

    // 1. 私有静态变量，用于保存MemConnect的单一实例
    private static volatile Transaction instance = null;

    // 3. 提供一个全局访问点
    public static Transaction getInstance(){
        // 双重检查锁定模式
        try {
            if (instance == null) { // 第一次检查
                synchronized (Transaction.class) {
                    if (instance == null) { // 第二次检查
                        instance = new Transaction();
                    }
                }
            }
            return instance;
        }catch (TMDBException e){
            logger.warn(e.getMessage());
        }catch (JSQLParserException e){
            logger.warn(e.getMessage());
        }catch (IOException e){
            logger.error(e.getMessage());
        }
        return instance;
    }

    private Transaction() throws IOException, JSQLParserException, TMDBException {
        // 防止通过反射创建多个实例
        if (instance != null) {
            throw new RuntimeException("Use getInstance() method to get the single instance of this class.");
        }
        this.mem = MemManager.getInstance();
        this.levelManager = MemManager.levelManager;
        this.memConnect=MemConnect.getInstance(mem);

    }


    public void clear() {
//        File classtab=new File("/data/data/edu.whu.tmdb/transaction/classtable");
//        classtab.delete();
        File objtab=new File("/data/data/edu.whu.tmdb/transaction/objecttable");
        objtab.delete();
    }

    public void SaveAll( ) {
        memConnect.SaveAll();
    }

    public void reload() {
        memConnect.reload();
    }

    public void Test(){
        TupleList tpl = new TupleList();
        Tuple t1 = new Tuple();
        t1.tupleHeader = 5;
        t1.tuple = new Object[t1.tupleHeader];
        t1.tuple[0] = "a";
        t1.tuple[1] = 1;
        t1.tuple[2] = "b";
        t1.tuple[3] = 3;
        t1.tuple[4] = "e";
        Tuple t2 = new Tuple();
        t2.tupleHeader = 5;
        t2.tuple = new Object[t2.tupleHeader];
        t2.tuple[0] = "d";
        t2.tuple[1] = 2;
        t2.tuple[2] = "e";
        t2.tuple[3] = 2;
        t2.tuple[4] = "v";

        tpl.addTuple(t1);
        tpl.addTuple(t2);
        String[] attrname = {"attr2","attr1","attr3","attr5","attr4"};
        int[] attrid = {1,0,2,4,3};
        String[]attrtype = {"int","char","char","char","int"};

    }

    public SelectResult query(String s)  {
        SelectResult query = null;
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(s.getBytes());
            //使用JSqlparser进行sql语句解析，会根据sql类型生成对应的语法树。
            Statement stmt = CCJSqlParserUtil.parse(byteArrayInputStream);
            query = this.query("", -1, stmt);
        }
        catch (JSQLParserException e){
            System.out.println(e.getMessage());
        }
        return query;
    }

    public SelectResult query(Statement s) {
        SelectResult query = this.query("", -1, s);
        return query;
    }
    public SelectResult query(String k, int op, Statement stmt) {
        //Action action = new Action();
//        action.generate(s);
        ArrayList<Integer> tuples=new ArrayList<>();
        SelectResult selectResult = new SelectResult();
        try {
            //获取生成语法树的类型，用于进一步判断
            String sqlType=stmt.getClass().getSimpleName();

            switch (sqlType) {
                case "CreateTable":
//                    log.WrteLog(s);
                    Create create =new CreateImpl();
                    create.create(stmt);
                    break;
                case "CreateDeputyClass":
//                    switch
//                    log.WriteLog(id,k,op,s);
                    CreateDeputyClass createDeputyClass=new CreateDeputyClassImpl();
                    createDeputyClass.createDeputyClass(stmt);
                    break;
                case "CreateTJoinDeputyClass":
//                    switch
                    //                   log.WriteLog(id,k,op,s);
                    CreateTJoinDeputyClassImpl createTJoinDeputyClass=new CreateTJoinDeputyClassImpl();
                    createTJoinDeputyClass.createTJoinDeputyClass(stmt);
                    break;
                case "Drop":
//                    log.WriteLog(id,k,op,s);
                    Drop drop=new DropImpl();
                    drop.drop(stmt);
                    break;
                case "Insert":
//                    log.WriteLog(id,k,op,s);
                    Insert insert=new InsertImpl();
                    tuples=insert.insert(stmt);
                    break;
                case "Delete":
 //                   log.WriteLog(id,k,op,s);
                    Delete delete=new DeleteImpl();
                    tuples= delete.delete(stmt);
                    break;
                case "Select":
                    Select select=new SelectImpl();
                    selectResult=select.select((net.sf.jsqlparser.statement.select.Select) stmt);
                    for (Tuple t:
                         selectResult.getTpl().tuplelist) {
                         tuples.add(t.getTupleId());
                    }
                    break;
                case "Update":
 //                   log.WriteLog(id,k,op,s);
                    Update update=new UpdateImpl();
                    tuples=update.update(stmt);
                    break;
                default:
                    break;

            }
        } catch (JSQLParserException e) {
            logger.warn(e.getMessage());
        } catch (TMDBException e) {
            logger.warn(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        }
        int[] ints = new int[tuples.size()];
        for (int i = 0; i < tuples.size(); i++) {
            ints[i]=tuples.get(i);
        }
//        action.setKey(ints);
        return selectResult;
    }

    public void mapMatching(String baseDir, String trajSrc,String osmSrc) throws IOException {
        TorchConnect.init(memConnect,baseDir);

        TorchConnect.torchConnect.insert(trajSrc);
        this.SaveAll();

        TorchConnect.torchConnect.mapMatching(trajSrc,osmSrc);
        this.SaveAll();
    }

    public void initEngine(String baseDir){
        TorchConnect.init(memConnect,baseDir);
    }
    private void Engine(String baseDir,String querySrc) throws IOException {
        initEngine(baseDir);
        TorchConnect.torchConnect.initEngine();
        TorchConnect.torchConnect.test(querySrc);
    }

    private void MapMatching(String baseDir, String trajSrc, String osmSrc) {
        TorchConnect.init(memConnect,baseDir);
        TorchConnect.torchConnect.mapMatching(trajSrc,osmSrc);
    }

    private void insertIntoTrajTable(String baseDir,String src) {
        TorchConnect.init(memConnect,baseDir);
        TorchConnect.torchConnect.insert(src);
    }




}

