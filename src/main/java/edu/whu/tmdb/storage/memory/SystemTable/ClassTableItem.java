package edu.whu.tmdb.storage.memory.SystemTable;

import edu.whu.tmdb.query.enums.DataType;

import java.io.Serializable;
import java.util.Objects;

public class ClassTableItem implements Serializable {
    public String classname = "";        //类名
    public int classid = 0;                //类id
    public int attrnum = 0;                //类属性个数
    public int    attrid = 0;
    public String attrname = "";         //属性名
    public DataType attrtype;         //属性类型
    public String classtype = "";
    public String alias="";

    public ClassTableItem(String classname, int classid, int attrnum,int attrid, String attrname, DataType attrtype,String classtype,String alias) {
        this.classname = classname;
        this.classid = classid;
        this.attrnum = attrnum;
        this.attrname = attrname;
        this.attrtype = attrtype;
        this.attrid = attrid;
        this.classtype = classtype;
        this.alias = alias;
    }
    public ClassTableItem(){}

    public ClassTableItem getCopy(){
        return new ClassTableItem(this.classname,this.classid,this.attrnum,this.attrid,this.attrname,this.attrtype,this.classtype,this.alias);
    }

    @Override
    public boolean equals(Object object){
        if(this==object) return true;
        if (!(object instanceof ClassTableItem)) {
            return false;
        }
        ClassTableItem oi=(ClassTableItem) object;
        if(this.classid!=oi.classid){
            return false;
        }
        if(this.classname!=oi.classname){
            return false;
        }
        if(this.attrid!=oi.attrid){
            return false;
        }
        if(this.attrname!=oi.attrname){
            return false;
        }
        if(this.attrnum!=oi.attrnum){
            return false;
        }
        if(this.attrtype!=oi.attrtype){
            return false;
        }
        if(this.alias!=oi.alias){
            return false;
        }
        if(this.classtype!=oi.classtype){
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + Objects.hash(this.classid)
                +Objects.hash(this.classname)
                +Objects.hash(this.classtype)
                +Objects.hash(this.alias)
                +Objects.hash(this.attrid)
                +Objects.hash(this.attrname)
                +Objects.hash(this.attrnum)
                +Objects.hash(this.attrtype)
        ;
        return result;
    }


}
