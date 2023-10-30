package edu.whu.tmdb.query.utils;


import edu.whu.tmdb.query.enums.DataType;
import edu.whu.tmdb.storage.memory.TupleList;

public class SelectResult {
    TupleList tpl; //存实际的元祖
    String[] className; //每个列的类名
    String[] attrname; //列名
    String[] alias;//每列的别名，在进行select时会用到
    int[] attrid;// 显示时使用
    DataType[] type;//元素类型，char，int这种

    public SelectResult(TupleList tpl, String[] className,String[] attrname, String[] alias,int[] attrid, DataType[] type) {
        this.className=className;
        this.tpl = tpl;
        this.attrname = attrname;
        this.alias= alias;
        this.attrid = attrid;
        this.type = type;
    }

    public SelectResult(){}

    public String[] getClassName() {
        return className;
    }

    public String[] getAlias() {
        return alias;
    }

    public void setAlias(String[] alias) {
        this.alias = alias;
    }

    public void setClassName(String[] className) {
        this.className = className;
    }

    public TupleList getTpl() {
        return tpl;
    }

    public void setTpl(TupleList tpl) {
        this.tpl = tpl;
    }

    public String[] getAttrname() {
        return attrname;
    }

    public void setAttrname(String[] attrname) {
        this.attrname = attrname;
    }

    public int[] getAttrid() {
        return attrid;
    }

    public void setAttrid(int[] attrid) {
        this.attrid = attrid;
    }

    public DataType[] getType() {
        return type;
    }

    public void setType(DataType[] type) {
        this.type = type;
    }
}
