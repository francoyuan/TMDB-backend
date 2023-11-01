package edu.whu.tmdb.query.utils;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.SystemTable.*;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.utils.K;
import edu.whu.tmdb.storage.utils.V;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemConnect {
    //进行内存操作的一些一些方法和数据
    private static Logger logger = LoggerFactory.getLogger(MemConnect.class);
    private MemManager mem;
    public static ObjectTable topt;
    private static ClassTable classt;
    private static DeputyTable deputyt;
    private static BiPointerTable biPointerT;
    private static SwitchingTable switchingT;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // 1. 私有静态变量，用于保存MemConnect的单一实例
    private static volatile MemConnect instance = null;

    // 2. 私有构造函数，确保不能从类外部实例化
    private MemConnect() {
        // 防止通过反射创建多个实例
        if (instance != null) {
            throw new RuntimeException("Use getInstance() method to get the single instance of this class.");
        }
    }

    // 3. 提供一个全局访问点
    public static MemConnect getInstance(MemManager mem) {
        // 双重检查锁定模式
        if (instance == null) { // 第一次检查
            synchronized (MemConnect.class) {
                if (instance == null) { // 第二次检查
                    instance = new MemConnect(mem);
                }
            }
        }
        return instance;
    }

    private MemConnect(MemManager mem) {
        this.mem = mem;
        topt = MemManager.objectTable;
        classt = MemManager.classTable;
        deputyt = MemManager.deputyTable;
        biPointerT = MemManager.biPointerTable;
        switchingT = MemManager.switchingTable;
    }



    //获取tuple
    public Tuple GetTuple(int id) {
        Tuple t = null;
            Object searchResult = this.mem.search(new K("t" + id));
            if (searchResult == null)
                t= null;
            if (searchResult instanceof Tuple)
                t = (Tuple) searchResult;
            else if (searchResult instanceof V)
//                t= JSON.parseObject(((V) searchResult).valueString, Tuple.class);
                t= (Tuple) KryoSerialization.deserializeFromString(((V) searchResult).valueString);
        if (t.delete)
                t= null;
            return t;
    }

    //插入tuple
    public void InsertTuple(Tuple tuple) {
        this.mem.add(tuple);
    }



    //删除tuple
    public void DeleteTuple(int id) {
        rwLock.writeLock().lock();
        try {
            if (id >= 0) {
                Tuple tuple = new Tuple();
                tuple.tupleId = id;
                tuple.delete = true;
                mem.add(tuple);
            }
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    //更新tuple
    public void UpateTuple(Tuple tuple, int tupleId) {
        rwLock.writeLock().unlock();
        try {
            tuple.tupleId = tupleId;
            this.mem.add(tuple);
        }finally {
            rwLock.writeLock().unlock();
        }
    }

//   获取表在classTable中的id值
    public int getClassId(String fromItem) throws TMDBException {
        for (ClassTableItem item : classt.classTable) {
            if (item.classname.equals(fromItem)) {
                return item.classid;
            }
        }
        return -1;
    }

    public boolean Condition(String attrtype, Tuple tuple, int attrid, String value1) {
        String value = value1.replace("\"", "");
        switch (attrtype) {
            case "int":
                int value_int = Integer.parseInt(value);
                if (Integer.parseInt((String) tuple.tuple[attrid]) == value_int)
                    return true;
                break;
            case "char":
                String value_string = value;
                if (tuple.tuple[attrid].equals(value_string))
                    return true;
                break;

        }
        return false;
    }

    public void SaveAll() {
        mem.saveAll();
    }

    public void reload() {
        try {
            mem.loadClassTable();
            mem.loadDeputyTable();
            mem.loadBiPointerTable();
            mem.loadSwitchingTable();
        }catch (IOException e){
            logger.error(e.getMessage());
        }
    }



    public static ObjectTable getTopt() {
        return topt;
    }

    public static ClassTable getClasst() {
        return classt;
    }

    public static DeputyTable getDeputyt() {
        return deputyt;
    }

    public static BiPointerTable getBiPointerT() {
        return biPointerT;
    }

    public static SwitchingTable getSwitchingT() {
        return switchingT;
    }
}
