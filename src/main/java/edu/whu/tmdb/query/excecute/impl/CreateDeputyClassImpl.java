package edu.whu.tmdb.query.excecute.impl;


import edu.whu.tmdb.query.enums.DeputyType;
import edu.whu.tmdb.query.excecute.CreateDeputyClass;
import edu.whu.tmdb.storage.memory.MemManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.HashSet;
import java.util.List;

import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.query.utils.TMDBException;
import edu.whu.tmdb.query.utils.MemConnect;
import edu.whu.tmdb.query.utils.SelectResult;


public class CreateDeputyClassImpl implements CreateDeputyClass {
    private MemConnect memConnect;

    public CreateDeputyClassImpl() {
        this.memConnect=MemConnect.getInstance(MemManager.getInstance());
    }

    @Override
    public boolean createDeputyClass(Statement stmt) throws TMDBException, IOException {
        return execute((net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass) stmt);
    }
    public boolean execute(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt) throws TMDBException, IOException {
        //获取新创建代理类的名称
        String deputyClass=stmt.getDeputyClass().toString();
        int deputyType=getDeputyType(stmt);
        Select select=stmt.getSelect();
        //获取select语句的selectResult
        SelectResult selectResult=getSelectResult(select);
        if(memConnect.getClassId(deputyClass)!=-1){
            throw new TMDBException(deputyClass+" already exists");
        }
        return createDeputyClassStreamLine(selectResult,deputyType,deputyClass);
    }



    public boolean createDeputyClassStreamLine(SelectResult selectResult, int deputyType, String deputyClass) throws TMDBException, IOException {
        int deputyId = createDeputyClass(deputyClass, selectResult, deputyType);
        insertDeputyTable(selectResult.getClassName(),deputyType,deputyId);
        insertTuple(selectResult,deputyId);
        return true;
    }



    private List<String> getColumns(Select select) {
        SelectImpl select1=new SelectImpl();
        ArrayList<SelectItem> selectItemList=(ArrayList<SelectItem>)((PlainSelect)select.getSelectBody()).getSelectItems();
        HashMap<SelectItem,ArrayList<Column>> selectItemToColumn=select1.getSelectItemColumn(selectItemList);
        List<Column> selectColumnList=select1.getSelectColumnList(selectItemToColumn);
        List<String> res=new ArrayList<>();
        for (int i = 0; i < selectColumnList.size(); i++) {
            res.add(selectColumnList.get(i).getColumnName());
        }
        return res;
    }

    private int getDeputyType(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt) {
        DeputyType deputyType = DeputyType.fromValue(stmt.getType().toLowerCase(Locale.ROOT));
        switch (deputyType) {
            case SELECT_DEPUTY: return deputyType.getIntValue();
            case JOIN_DEPUTY: return deputyType.getIntValue();
            case UNION_DEPUTY: return deputyType.getIntValue();
            case GROUPBY_DEPUTY: return deputyType.getIntValue();
            default: throw new IllegalArgumentException("Unknown type: " + stmt.getType());
        }
    }

    //将创建deputyclass后面的select语句中的selectResult拿到，用于后面的处理
     private SelectResult getSelectResult(Select select) throws TMDBException, IOException {
         SelectImpl select1 = new SelectImpl();
        SelectResult selectResult=select1.select(select);
        return selectResult;
    }

    //第一步，创建代理类，代理类的classtype设置为de
    //同时，在switchingtable中插入源属性到代理属性的映射
    private int createDeputyClass(String deputyClassName, SelectResult selectResult, int deputyRule) throws TMDBException {
        MemConnect.getClasst().maxid++;
        int classid = MemConnect.getClasst().maxid;//代理类的id
        int count=selectResult.getAttrid().length;//代理类的长度
        for (int i = 0; i < selectResult.getAttrid().length; i++) {
            MemConnect.getClasst().classTable.add(
                    new ClassTableItem(deputyClassName,
                                        classid,
                                        count,
                                        selectResult.getAttrid()[i],
                                        selectResult.getAttrname()[i],
                                        selectResult.getType()[i],
                                        "de",
                                        ""));
            String className=selectResult.getClassName()[i];
            int oriId=memConnect.getClassId(className);
            int oriAttrId=getOriAttrId(oriId,selectResult.getAlias()[i]);
            MemConnect.getSwitchingT().switchingTable.add(
                    new SwitchingTableItem(oriId,oriAttrId,selectResult.getAlias()[i],classid,i,selectResult.getAttrname()[i],deputyRule+"")
            );
        }
        return classid;
    }

    /**
     * get the origin class classid
     * @param oriId
     * @param alias
     * @return
     */
    private int getOriAttrId(int oriId, String alias) {
        for (int i = 0; i < MemConnect.getClasst().classTable.size(); i++) {
            ClassTableItem classTableItem = MemConnect.getClasst().classTable.get(i);
            if(classTableItem.classid==oriId && classTableItem.attrname.equals(alias)){
                return classTableItem.attrid;
            }
        }
        return -1;
    }

    /**
     * 第二步，在deputytable中插入具体的tuple
     * @param className deputy class's className
     * @param deputyType deputy class's deputytype
     * @param deputyId
     * @throws TMDBException
     */
    public void insertDeputyTable(String[] className,int deputyType, int deputyId) throws TMDBException {
        HashSet<String> collect = Arrays.stream(className).collect(Collectors.toCollection(HashSet::new));
        for (String s :
                collect) {
            int oriId=memConnect.getClassId(s);
            MemConnect.getDeputyt().deputyTable.add(
                    new DeputyTableItem(oriId,deputyId,new String[]{deputyType+""})
            );
        }
    }

    //第三步，在ObjectTable中插入实际值
    private void insertTuple(SelectResult selectResult, int deputyId) throws TMDBException, IOException {
        InsertImpl insert=new InsertImpl();
        List<String> columns= Arrays.asList(selectResult.getAttrname());
        for (int i = 0; i < selectResult.getTpl().tuplelist.size(); i++) {
            Tuple tuple=selectResult.getTpl().tuplelist.get(i);
            int deputyTupleId = insert.executeTuple(deputyId, columns, new Tuple(tuple.tuple));
            HashSet<Integer> origin = getOriginClass(selectResult);
            for (int o :
                    origin) {
                int classId=memConnect.getClassId(selectResult.getClassName()[o]);
                int oriTupleId=tuple.tupleIds[o];
                MemConnect.getBiPointerT().biPointerTable.add(
                        new BiPointerTableItem(classId,oriTupleId,deputyId,deputyTupleId)
                );
            }
        }
    }
    
    private HashSet<Integer> getOriginClass(SelectResult selectResult){
        ArrayList<String> collect = Arrays.stream(selectResult.getClassName()).collect(Collectors.toCollection(ArrayList::new));
        HashSet<String> collect1 = Arrays.stream(selectResult.getClassName()).collect(Collectors.toCollection(HashSet::new));
        HashSet<Integer> res=new HashSet<>();
        for(String s:collect1){
            res.add(collect.indexOf(s));
        }
        return res;
    } 


}
