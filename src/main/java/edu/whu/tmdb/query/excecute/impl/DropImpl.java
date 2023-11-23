package edu.whu.tmdb.query.excecute.impl;

import edu.whu.tmdb.query.excecute.Drop;
import edu.whu.tmdb.storage.memory.MemManager;
import net.sf.jsqlparser.statement.Statement;

import java.util.ArrayList;

import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ObjectTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.query.utils.TMDBException;
import edu.whu.tmdb.query.utils.MemConnect;

public class DropImpl implements Drop {
    private MemConnect memConnect;


    public DropImpl() {
        this.memConnect=MemConnect.getInstance(MemManager.getInstance());
    }

    @Override
    public boolean drop(Statement statement) throws TMDBException {
        return execute((net.sf.jsqlparser.statement.drop.Drop) statement);
    }

    public boolean execute(net.sf.jsqlparser.statement.drop.Drop drop) throws TMDBException {
        String dropTable=drop.getName().getName();
        int classId=memConnect.getClassId(dropTable);
        drop(classId);
        return true;
    }

    public void drop(int classId){
        ArrayList<ClassTableItem> tempC=new ArrayList<>();
        for (int i = 0; i < MemConnect.getClasst().classTable.size(); i++) {
            ClassTableItem classTableItem = MemConnect.getClasst().classTable.get(i);
            if(classTableItem.classid==classId){
                tempC.add(classTableItem);
            }
        }
        for (ClassTableItem temp :
                tempC) {
            MemConnect.getClasst().classTable.remove(temp);
        }
        ArrayList<DeputyTableItem> tempD=new ArrayList<>();
        ArrayList<Integer> toDrop=new ArrayList<>();
        for (int i = 0; i < MemConnect.getDeputyt().deputyTable.size(); i++) {
            DeputyTableItem deputyTableItem = MemConnect.getDeputyt().deputyTable.get(i);
            if(deputyTableItem.originid==classId){
                toDrop.add(deputyTableItem.deputyid);
                tempD.add(deputyTableItem);
            }
        }
        for(DeputyTableItem temp: tempD){
            MemConnect.getDeputyt().deputyTable.remove(temp);
        }
        ArrayList<BiPointerTableItem> tempB=new ArrayList<>();
        for (int i = 0; i < MemConnect.getBiPointerT().biPointerTable.size(); i++) {
            BiPointerTableItem biPointerTableItem = MemConnect.getBiPointerT().biPointerTable.get(i);
            if(biPointerTableItem.objectid==classId || biPointerTableItem.deputyobjectid==classId){
                tempB.add(biPointerTableItem);
            }
        }
        for(BiPointerTableItem temp:tempB){
            MemConnect.getBiPointerT().biPointerTable.remove(temp);
        }
        ArrayList<SwitchingTableItem> tempS=new ArrayList<>();
        for (int i = 0; i < MemConnect.getSwitchingT().switchingTable.size(); i++) {
            SwitchingTableItem switchingTableItem = MemConnect.getSwitchingT().switchingTable.get(i);
            if(switchingTableItem.oriId==classId || switchingTableItem.deputyId==classId){
                tempS.add(switchingTableItem);
            }
        }
        for(SwitchingTableItem temp:tempS){
            MemConnect.getSwitchingT().switchingTable.remove(temp);
        }
        ArrayList<ObjectTableItem> tempT=new ArrayList<>();
        for (int i = 0; i < MemConnect.getTopt().objectTable.size(); i++) {
            ObjectTableItem objectTableItem = MemConnect.getTopt().objectTable.get(i);
            if(objectTableItem.classid==classId ){
                memConnect.DeleteTuple(objectTableItem.tupleid);
                tempT.add(objectTableItem);
            }
        }
        for(ObjectTableItem temp:tempT){
            MemConnect.getTopt().objectTable.remove(temp);
        }
        if(toDrop.isEmpty()){
            return;
        }
        for (int i = 0; i < toDrop.size(); i++) {
            drop(toDrop.get(i));
        }
    }
}
