package edu.whu.tmdb.query.excecute.impl;


import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ObjectTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;

import edu.whu.tmdb.query.utils.TMDBException;
import edu.whu.tmdb.query.excecute.Delete;
import edu.whu.tmdb.query.excecute.Select;
import edu.whu.tmdb.query.utils.MemConnect;
import edu.whu.tmdb.query.utils.SelectResult;

public class DeleteImpl implements Delete {

    private MemConnect memConnect;
    private ArrayList<Integer> deleteId=new ArrayList<>();

    public DeleteImpl() {
        this.memConnect=MemConnect.getInstance(MemManager.getInstance());
    }

    @Override
    public ArrayList<Integer> delete(Statement statement) throws JSQLParserException, TMDBException, IOException {
        return execute((net.sf.jsqlparser.statement.delete.Delete) statement);
    }

    public ArrayList<Integer> execute(net.sf.jsqlparser.statement.delete.Delete delete) throws JSQLParserException, TMDBException, IOException {
        //获取需要删除的表名
        Table table = delete.getTable();
        //获取delete中的where表达式
        Expression where = delete.getWhere();
        String sql="select * from " + table + " where " + where.toString() + ";";
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(sql.getBytes());
        net.sf.jsqlparser.statement.select.Select parse = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil.parse(byteArrayInputStream);
        Select select=new SelectImpl();
        SelectResult selectResult = select.select(parse);
        ArrayList<Integer> integers = new ArrayList<>();
//        int classid=memConnect.getClassId(table.getName());
        delete(selectResult.getTpl());
        return integers;
    }

    public void delete(TupleList tupleList){
        ArrayList<Integer> delete=new ArrayList<>();
        for(Tuple tuple:tupleList.tuplelist){
            memConnect.DeleteTuple(tuple.getTupleId());
            ObjectTableItem o=new ObjectTableItem(tuple.classId,tuple.getTupleId());
//            ArrayList<Object> list=memConnect.getTopt().objectTable;
            MemConnect.getTopt().objectTable.remove(o);
            delete.add(tuple.getTupleId());
        }
        deleteId.addAll(delete);
        ArrayList<Integer> toDelete=new ArrayList<>();
        for (int i = 0; i < MemConnect.getBiPointerT().biPointerTable.size(); i++) {
            BiPointerTableItem biPointerTableItem = MemConnect.getBiPointerT().biPointerTable.get(i);
            if(delete.contains(biPointerTableItem.objectid)){
                toDelete.add(biPointerTableItem.deputyobjectid);
                MemConnect.getBiPointerT().biPointerTable.remove(biPointerTableItem);
            }
        }
        if(toDelete.isEmpty()){
            return;
        }
        TupleList tupleList1 = new TupleList();
        for (int i = 0; i < toDelete.size(); i++) {
            Tuple tuple = memConnect.GetTuple(toDelete.get(i));
            tupleList1.addTuple(tuple);
        }
        delete(tupleList1);
    }

}
