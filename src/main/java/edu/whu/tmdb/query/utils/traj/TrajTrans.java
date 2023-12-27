package edu.whu.tmdb.query.utils.traj;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import au.edu.rmit.bdm.Torch.base.model.Coordinate;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.base.model.Trajectory;
import edu.whu.tmdb.query.excecute.Select;
import edu.whu.tmdb.query.utils.SelectResult;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;


public class TrajTrans {
    public static String getString(List<Coordinate> list){
        String temps="";
        for (int k = 0; k < list.size()-1; k++) {
            Coordinate coordinate = list.get(k);
            temps+=coordinate.lat+"|"+coordinate.lng+"|";
        }
        temps+=list.get(list.size()-1).lat+"|"
                +list.get(list.size()-1).lng;
        return temps;
    }

    public static List<TrajEntry> getTraj(double[] a){
        ArrayList<TrajEntry> res = new ArrayList<>();
        for (int i = 0; i < a.length; i+=2) {
            res.add(new Coordinate(a[i+1],a[i]));
        }
        return res;
    }

    public static List<TrajEntry> getTraj(String[] a){
        ArrayList<TrajEntry> res = new ArrayList<>();
        for (int i = 0; i < a.length; i+=2) {
            res.add(new Coordinate(Double.parseDouble(a[i+1]),Double.parseDouble(a[i])));
        }
        return res;
    }


    public static String getTorchTraj(String s){
        String[] split = s.split("\\|");
        StringBuilder sb=new StringBuilder("[");
        for (int i = 0; i < split.length-2; i+=2) {
            sb.append("[")
                    .append(split[i])
                    .append(",")
                    .append(split[i+1])
                    .append("]")
                    .append(",");
        }
        sb.append("[")
                .append(split[split.length-2])
                .append(",")
                .append(split[split.length-1])
                .append("]");
        sb.append("]");
        return sb.toString();
    }

    public static SelectResult getSelectResultByTrajList(List<Trajectory<TrajEntry>> list, SelectResult selectResult){
        TupleList tupleList=new TupleList();
        for (int i = 0; i < list.size(); i++) {
            List<TrajEntry> trajEntries = list.get(i);
            Tuple tuple=new Tuple();
            tuple.tuple=new Object[]{i,-1,"",trajEntries.toString()};
            tuple.tupleIds=new int[4];
            tupleList.tuplelist.add(tuple);
        }
        selectResult.setTpl(tupleList);
        return selectResult;
    }


    public static SelectResult getSelectResultByList(List<List<TrajEntry>> list, SelectResult selectResult){
        TupleList tupleList=new TupleList();
        for (int i = 0; i < list.size(); i++) {
            List<TrajEntry> trajEntries = list.get(i);
            Tuple tuple=new Tuple();
            tuple.tuple=new Object[]{i,-1,"",trajEntries.toString()};
            tuple.tupleIds=new int[4];
            tupleList.tuplelist.add(tuple);
        }
        selectResult.setTpl(tupleList);
        return selectResult;
    }
    
    public static List<List<TrajEntry>> getTrajListBySelectResult(SelectResult selectResult){
        String[] attrname = selectResult.getAttrname();
        int id=-1;
        for (int i = 0; i < attrname.length; i++) {
            if(attrname[i].equals("traj")){
                id=i;
            }
        }
        List<List<TrajEntry>> res=new ArrayList<>();
        for (Tuple t:
             selectResult.getTpl().tuplelist) {

            res.add(getTraj((double[]) t.tuple[id]));
        }
        return res;
    }

    public static List<List<TrajEntry>> getTrajListBySelectResult(SelectResult selectResult,int limit){
        String[] attrname = selectResult.getAttrname();
        int id=-1;
        for (int i = 0; i < attrname.length; i++) {
            if(attrname[i].equals("traj")){
                id=i;
            }
        }
        List<List<TrajEntry>> res=new ArrayList<>();
        for (Tuple t:
                selectResult.getTpl().tuplelist) {
            res.add(getTraj((double[]) t.tuple[id]));
            if(limit--<=0){
                break;
            }
        }
        return res;
    }

    public static SelectResult getTrajsByIds(SelectResult selectResult,int[] ids){
        Set<Integer> set = Arrays.stream(ids).boxed().collect(Collectors.toSet());
        TupleList tupleList = new TupleList();

        for (Tuple tuple
                : selectResult.getTpl().tuplelist) {
            if (set.contains(tuple.tuple[0])) {
                tupleList.addTuple(tuple);
            }
        }
        selectResult.setTpl(tupleList);
        return selectResult;
    }

}
