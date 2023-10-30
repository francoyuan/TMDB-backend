package edu.whu.tmdb.query.excecute.impl;

import edu.whu.tmdb.query.enums.DataType;
import edu.whu.tmdb.storage.memory.MemManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.ArrayList;

import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.query.utils.TMDBException;
import edu.whu.tmdb.query.excecute.Create;
import edu.whu.tmdb.query.utils.MemConnect;

public class CreateImpl implements Create {
    private MemConnect memConnect;
    public CreateImpl() {
        this.memConnect=MemConnect.getInstance(MemManager.getInstance());
    }


    @Override
    public boolean create(Statement stmt) throws TMDBException {
        return execute((CreateTable) stmt);
    }

    public boolean execute(CreateTable stmt) throws TMDBException {
        //获取新定义class具体元素
        ArrayList<ColumnDefinition> columnDefinitionArrayList= (ArrayList<ColumnDefinition>) stmt.getColumnDefinitions();
        String classname = stmt.getTable().toString();
        int count = columnDefinitionArrayList.size();
        MemConnect.getClasst().maxid++;
        int classid = MemConnect.getClasst().maxid;
        for(ClassTableItem item : MemConnect.getClasst().classTable){
            if(item.classname.equals(classname)){
                throw new TMDBException("table "+classname+"已经存在！");
            }
        }
        for (int i = 0; i < count; i++) {
            ClassTableItem ori = new ClassTableItem(classname, classid, count, i,
                    columnDefinitionArrayList.get(i).getColumnName(),
                    DataType.mapToDataType(columnDefinitionArrayList.get(i).toStringDataTypeAndSpec()).get()
                    , "ori", "");
            MemConnect.getClasst().classTable.add(ori);
        }
        return true;
    }
}
