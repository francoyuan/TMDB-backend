package edu.whu.tmdb.query.excecute.impl;


import au.edu.rmit.bdm.Torch.base.model.Coordinate;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import edu.whu.tmdb.storage.memory.MemManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


import edu.whu.tmdb.query.utils.TMDBException;
import edu.whu.tmdb.query.excecute.Insert;
import edu.whu.tmdb.query.utils.MemConnect;
import edu.whu.tmdb.query.utils.SelectResult;
import edu.whu.tmdb.query.utils.traj.TrajTrans;
import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ObjectTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;

public class InsertImpl implements Insert {
    private MemConnect memConnect;

    ArrayList<Integer> indexs=new ArrayList<>();

    public InsertImpl() {
        this.memConnect=MemConnect.getInstance(MemManager.getInstance());
    }

    @Override
    public ArrayList<Integer> insert(Statement stmt) throws TMDBException, IOException {
        net.sf.jsqlparser.statement.insert.Insert statement=(net.sf.jsqlparser.statement.insert.Insert) stmt;
        //获取插入的table名
        Table table= statement.getTable();
        //获取插入的column的名称

        List<String> columns=new ArrayList<>();
        if(statement.getColumns()==null){
            columns=getColumns(table.getName());
        }
        else{
            for (int i = 0; i < statement.getColumns().size(); i++) {
                columns.add(statement.getColumns().get(i).getColumnName());
            }
        }
        //插入的TupleList
        SelectImpl select=new SelectImpl();

        //insert后面的values是一个select节点，获取values或者其它类型select的值
        SelectResult selectResult=select.select(statement.getSelect());
        //tuplelist就是SelectResult中实际存储tuple部分
        TupleList tupleList=selectResult.getTpl();
        execute(table.getName(),columns,tupleList);
        return indexs;
    }

    public void execute(String table, List<String> columns, TupleList tupleList) throws TMDBException, IOException {
        int classId=memConnect.getClassId(table);
        int l=getLength(classId);
        int[] index=getTemplate(classId,columns);
        for (int i = 0; i < tupleList.tuplelist.size(); i++) {
            indexs.add(insert(classId,columns,tupleList.tuplelist.get(i),l,index));
        }
    }
    public void execute(int classId, List<String> columns, TupleList tupleList) throws TMDBException, IOException {
        int l=getLength(classId);
        int[] index=getTemplate(classId,columns);
        for (int i = 0; i < tupleList.tuplelist.size(); i++) {
            indexs.add(insert(classId,columns,tupleList.tuplelist.get(i),l,index));
        }
    }

    public int executeTuple(int classId, List<String> columns, Tuple tuple) throws TMDBException, IOException {
        int l=getLength(classId);
        int[] index=getTemplate(classId,columns);
        int insert = insert(classId, columns, tuple, l, index);
        indexs.add(insert);
        return insert;
    }


    /**
     * 分为几个阶段
     * 第一阶段插入tuple
     * @param classId
     * @param columns
     * @param tuple
     * @param l
     * @param index
     * @return
     * @throws TMDBException
     */
    private Integer insert(int classId, List<String> columns, Tuple tuple, int l, int[] index) throws TMDBException, IOException {
        //插入tuple
        SelectImpl select=new SelectImpl();
        int tupleid = MemConnect.getTopt().maxTupleId++;
        Object[] temp=new Object[l];
        if(tuple.tuple.length!=columns.size()){
            throw new TMDBException("Insert error: columns size not equals to tuple size");
        }
        for (int i = 0; i < index.length; i++) {
            temp[index[i]]=tuple.tuple[i];
        }
        tuple.classId=classId;
        tuple.tuple=temp;
        tuple.tupleHeader=tuple.tuple.length;
        int[] ids=new int[tuple.tupleHeader];
        Arrays.fill(ids,tupleid);
        tuple.tupleIds=ids;
        tuple.tupleId=tupleid;
        memConnect.InsertTuple(tuple);
        MemConnect.getTopt().objectTable.add(new ObjectTableItem(classId,tupleid));

        //往代理类中进行插入
        ArrayList<Integer> pointTo = deputyTable(classId);
        //得到源类到每个代理类具体的属性对应关系。
        ArrayList<HashMap<Integer, Integer>> deputyAttr = getDeputyAttr(pointTo.size(), classId);
        //找到插入的类对应的代理类进行遍历
        for (int i = 0; i < pointTo.size(); i++) {
            int tempClassId=pointTo.get(i);
            HashMap<String,String> tempMap=trans(deputyAttr.get(i),classId,tempClassId);
            List<String> tempColumns=getInsertColumns(classId,tempClassId,tempMap,columns);
            Tuple tuple1=getDeputyTuple(tempMap,tuple,columns);
            int i1 = executeTuple(tempClassId, tempColumns, tuple1);
            MemConnect.getBiPointerT().biPointerTable.add(
                    new BiPointerTableItem(classId,tupleid,tempClassId,i1)
            );
        }

        LongestCommonSubSequence longestCommonSubSequence=new LongestCommonSubSequence();
        //现在由于多了基于轨迹相似度的代理类，因此，需要进行额外的逻辑处理
        //首先调用tJoinDeputySize方法得到是否当前类是否存在基于轨迹相似度的代理类，并得到代理类的id list
        ArrayList<Integer> tJoinDeputy = tJoinDeputySize(classId);
        //如果list非空就需要进一步判断
        if(!tJoinDeputy.isEmpty()) {
            //调用TrajTrans.getTraj方法将当前元祖的轨迹部分转化为List<Coordinate> traj1进行后续操作
            List<TrajEntry> traj1 = TrajTrans.getTraj((double[]) tuple.tuple[2]);
            //对代理类idlist进行遍历
            for (int i = 0; i < tJoinDeputy.size(); i++) {
                //得到当前代理类的id
                int deputyId = tJoinDeputy.get(i);
                //由于基于轨迹相似度的代理类需要两个源类，需要拿到另一个源类，这里使用getAnotherDeputy方法拿到
                int an = getAnotherDeputy(deputyId, classId);
                //通过select的getTable（classid）方法拿到另一个源类的所有tuple
                TupleList table = select.getTable(an);
                //遍历另一个源类的所有tuple
                for (int j = 0; j < table.tuplelist.size(); j++) {
                    //拿到另一个源类的当前tuple
                    Tuple tuple1 = table.tuplelist.get(j);
                    //通过TrajTrans.getTraj方法将当前元祖的轨迹部分转化为List<Coordinate> traj2进行后续操作
                    List<TrajEntry> traj2 = TrajTrans.getTraj((double[]) tuple1.tuple[2]);
                    //通过longestCommonSubSequence.getCommonSubsequence获取当前两个traj的公共子序列
                    List<Coordinate> commonSubsequence = longestCommonSubSequence.getCommonSubsequence(traj1, traj2, 3);
                    //如果子序列长度大于阈值，则需要在代理类中插入新的tuple
                    if (commonSubsequence.size() > 1) {
                        //新建临时tuple，这个tuple就是要往代理类中进行插入的tuple
                        //临时tuple除了轨迹部分存的和当前进行插入的tuple（方法形参里的不同，其它都相同）步骤和TJoinSelect一样
                        Tuple temp1=new Tuple();
                        temp1.tupleId=tuple.tupleId;
                        temp1.tupleIds=tuple.tupleIds;
                        temp1.tuple=tuple.tuple;
                        //需要将得到的轨迹子序列，转换成string的形式，然后将tuple中轨迹部分设置为转换后的值
                        String temps=TrajTrans.getString(commonSubsequence);
                        temp1.tuple[2]=temps;
                        //调用一下方法在代理类中也插入新tuple
                        int i1 = executeTuple(deputyId, columns, temp1);
                        MemConnect.getBiPointerT().biPointerTable.add(
                                new BiPointerTableItem(classId, tupleid, deputyId, i1)
                        );
                    }
                }
            }
        }
        return tupleid;
    }

    private int getAnotherDeputy(int deputyId, int classId) {
        for (int i = 0; i < MemConnect.getDeputyt().deputyTable.size(); i++) {
            DeputyTableItem deputyTableItem = MemConnect.getDeputyt().deputyTable.get(i);
            if("5".equals(deputyTableItem.deputyrule[0]) &&
            deputyTableItem.originid!=classId &&
            deputyTableItem.deputyid==deputyId){
                return deputyTableItem.originid;
            }
        }
        return -1;
    }

    /**
     * 将属性id与属性id的对应关系，转换成是属性名到属性名的对应关系。
     * @param map
     * @param classId
     * @param tempClassId
     * @return
     */
    private HashMap<String, String> trans(HashMap<Integer, Integer> map, int classId, int tempClassId) {
        HashMap<String,String> map2=new HashMap<>();
        HashMap<Integer,String> tempmap1=new HashMap<>();
        HashMap<Integer,String> tempmap2=new HashMap<>();
        for (int i = 0; i < MemConnect.getClasst().classTable.size(); i++) {
            ClassTableItem classTableItem = MemConnect.getClasst().classTable.get(i);
            if(classTableItem.classid==classId){
                tempmap1.put(classTableItem.attrid,classTableItem.attrname);
            }
            if(classTableItem.classid==tempClassId){
                tempmap2.put(classTableItem.attrid,classTableItem.attrname);
            }
        }
        for(int i:map.keySet()){
            map2.put(tempmap1.get(i),tempmap2.get(map.get(i)));
        }
        return map2;
    }

    private Tuple getDeputyTuple(HashMap<String, String> map, Tuple tuple, List<String> columns) {
        Tuple res=new Tuple();
        Object[] temp=new Object[map.size()];
        int i=0;
        for(String s:columns){
            if (map.containsKey(s)) {
                temp[i]=tuple.tuple[columns.indexOf(s)];
                i++;
            }
        }
        res.tuple=temp;
        return res;
    }

    private List<String> getInsertColumns(int classId, int tempClassId, HashMap<String, String> map, List<String> columns) {

        List<String> res=new ArrayList<>();
        for (String column :
                columns) {
            if(map.containsKey(column)){
                res.add(map.get(column));
            }
        }
        return res;
    }

    public List<String> getColumns(String tableName){
        List<String> res=new ArrayList<>();
        for (int i = 0; i < MemConnect.getClasst().classTable.size(); i++) {
            ClassTableItem classTableItem = MemConnect.getClasst().classTable.get(i);
            if(classTableItem.classname.equals(tableName)){
                res.add(classTableItem.attrname);
            }
        }
        return res;
    }

    /**
     * 得到源类和每个对应代理类的属性id map
     * @param deputySize
     * @param oriId
     * @return
     */
    private ArrayList<HashMap<Integer,Integer>> getDeputyAttr(int deputySize, int oriId){
        int i=0;
        int c=-1;
        ArrayList<HashMap<Integer,Integer>> res=new ArrayList<>();
        while(i< MemConnect.getSwitchingT().switchingTable.size()){
            SwitchingTableItem switchingTableItem = MemConnect.getSwitchingT().switchingTable.get(i);
            if(switchingTableItem.oriId==oriId){
                c=switchingTableItem.deputyId;
                HashMap<Integer,Integer> map=new HashMap<>();
                while(i< MemConnect.getSwitchingT().switchingTable.size() &&
                        MemConnect.getSwitchingT().switchingTable.get(i).deputyId==c){
                    map.put(MemConnect.getSwitchingT().switchingTable.get(i).oriAttrid,
                            MemConnect.getSwitchingT().switchingTable.get(i).deputyAttrId);
                    i++;
                }
                res.add(map);
            }
            else{
                i++;
            }
        }
        return res;
    }

    private int getLength(int classId) {
        for (int i = 0; i < MemConnect.getClasst().classTable.size(); i++) {
            if(MemConnect.getClasst().classTable.get(i).classid==classId){
                return MemConnect.getClasst().classTable.get(i).attrnum;
            }
        }
        try {
            throw  new TMDBException("Insert error: class doesn't exist");
        } catch (TMDBException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private int[] getTemplate(int classId, List<String> columns) {
        HashMap<Integer,ClassTableItem> map=new HashMap<>();
        ArrayList<ClassTableItem> list=new ArrayList<>();
        for (int j = 0; j < MemConnect.getClasst().classTable.size(); j++) {
            ClassTableItem classTableItem = MemConnect.getClasst().classTable.get(j);
            if(classTableItem.classid==classId){
                list.add(classTableItem);
            }
        }
        int[] res=new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            for (int j = 0; j < list.size(); j++) {
                ClassTableItem classTableItem = list.get(j);
                if(classTableItem.attrname.equals(columns.get(i))){
                    res[i]=classTableItem.attrid;
                    break;
                }
            }
        }
        return res;
    }

    /**
     * 获取源类对应的代理类
     * @param classId
     * @return
     */
    private ArrayList<Integer> deputyTable(int classId) {
        ArrayList<Integer> deputy=new ArrayList<>();
        for (int i = 0; i < MemConnect.getDeputyt().deputyTable.size(); i++) {
            DeputyTableItem deputyTableItem = MemConnect.getDeputyt().deputyTable.get(i);
            if(deputyTableItem.originid==classId && !deputyTableItem.deputyrule[0].equals("5")){
                deputy.add(deputyTableItem.deputyid);
            }
        }
        return deputy;
    }

    private ArrayList<Integer> tJoinDeputySize(int classId) {
        ArrayList<Integer> deputy=new ArrayList<>();
        for (int i = 0; i < MemConnect.getDeputyt().deputyTable.size(); i++) {
            DeputyTableItem deputyTableItem = MemConnect.getDeputyt().deputyTable.get(i);
            if(deputyTableItem.originid==classId && deputyTableItem.deputyrule[0].equals("5")){
                deputy.add(deputyTableItem.deputyid);
            }
        }
        return deputy;
    }

    public int tupleInsert(int classId, Tuple tuple, boolean hasDeputy){
        int tupleid = MemConnect.getTopt().maxTupleId++;
        tuple.tupleHeader=tuple.tuple.length;
        int[] ids=new int[tuple.tupleHeader];
        Arrays.fill(ids,tupleid);
        tuple.tupleId=tupleid;
        memConnect.InsertTuple(tuple);
        MemConnect.getTopt().objectTable.add(new ObjectTableItem(classId,tupleid));
        if(hasDeputy){

        }
        return 1;
    }
}
