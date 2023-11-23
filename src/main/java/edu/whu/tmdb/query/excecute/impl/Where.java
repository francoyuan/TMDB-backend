package edu.whu.tmdb.query.excecute.impl;

import au.edu.rmit.bdm.Torch.base.model.Coordinate;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.base.model.Trajectory;
import au.edu.rmit.bdm.Torch.queryEngine.model.SearchWindow;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.query.torch.TorchConnect;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.IOException;
import java.util.*;

import edu.whu.tmdb.query.utils.TMDBException;
import edu.whu.tmdb.query.excecute.Select;
import edu.whu.tmdb.query.utils.Formula;
import edu.whu.tmdb.query.utils.MemConnect;
import edu.whu.tmdb.query.utils.SelectResult;
import edu.whu.tmdb.query.utils.traj.TrajTrans;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import org.apache.commons.math3.analysis.function.Exp;
import org.checkerframework.checker.units.qual.A;
import org.checkerframework.checker.units.qual.Length;

import static org.apache.commons.math3.distribution.fitting.MultivariateNormalMixtureExpectationMaximization.estimate;

public class  Where {
    private MemConnect memConnect;

    private List<List<Expression>> orExpressionList=new ArrayList<>();

    public Where() throws TMDBException, IOException {
        this.memConnect=MemConnect.getInstance(MemManager.getInstance());
    }

    Formula formula=new Formula();
    public SelectResult where(PlainSelect plainSelect, SelectResult selectResult) throws TMDBException, IOException {
        ArrayList<Expression> temp = new ArrayList<>();
        orExpressionList.add(temp);
        execute(plainSelect.getWhere(),temp);
        handleOr(selectResult);
        return selectResult;
    }

    public SelectResult handleOr(SelectResult selectResult) throws TMDBException, IOException {
        selectResult=handleAnd(orExpressionList.get(0),selectResult);
        for (int i = 1; i < orExpressionList.size(); i++) {
             selectResult = or(selectResult, handleAnd(orExpressionList.get(i), selectResult));
        }
        return selectResult;
    }

    public SelectResult handleAnd(List<Expression> expressions,SelectResult selectResult) throws TMDBException, IOException {
        if(expressions.size()>1){
           expressions=getOperationSequence(expressions);
        }
        for (int i = 0; i < expressions.size(); i++){
            Expression e = expressions.get(i);
            selectResult=execute(e,selectResult);
        }
        return selectResult;
    }

    public List<Expression> getOperationSequence(List<Expression> expressions){
        PriorityQueue<Object[]> queue=new PriorityQueue<>(new Comparator<Object[]>() {
            @Override
            public int compare(Object[] o1, Object[] o2) {
                return ((int)o1[0])-((int)o2[0]);
            }
        });
        for (int i = 0; i < expressions.size(); i++) {
            Expression e=expressions.get(i);
            queue.offer(new Object[]{estimate(e),e});
        }
        ArrayList<Expression> res=new ArrayList<>();
        while(!queue.isEmpty()){
            res.add((Expression) queue.poll()[1]);
        }
        return res;
    }

    public int estimate(Expression expression){
        switch(expression.getClass().getSimpleName()){
            case "InExpression": return 1;
            case "EqualsTo": return 10;
            case "MinorThan": return 100;
            case "GreaterThan": return 100;
            case "Function": return 1000+functionEstimate((Function)expression);
            default: return Integer.MAX_VALUE;
        }
    }

    public int functionEstimate(Function expression){
//        String name = expression.getName();
//        switch(name.toLowerCase()){
//            case "st_within": return
//            case "st_intersect": return
//            case "st_contain": return
//            case "st_similarity": return
//        }
//        return null;
        return 0;
    }




    public SelectResult execute(Expression expression,SelectResult selectResult) throws TMDBException, IOException {
        SelectResult res=new SelectResult();
//        if(selectResult.getTpl().tuplelist.isEmpty()) return selectResult;
        String a=expression.getClass().getSimpleName();
        switch (a){
            case "InExpression": res=inExpression((InExpression) expression,selectResult); break;
            case "EqualsTo": res=equalsToExpression((EqualsTo) expression,selectResult); break;
            case "MinorThan": res=minorThan((MinorThan) expression,selectResult); break;
            case "GreaterThan": res=greaterThan((GreaterThan) expression,selectResult); break;
            case "Function": res=function((Function)expression,selectResult); break;
        }
        return res;
    }

    public void execute(Expression expression,ArrayList<Expression> list) throws TMDBException, IOException {
        SelectResult res=new SelectResult();
//        if(selectResult.getTpl().tuplelist.isEmpty()) return selectResult;
        String a=expression.getClass().getSimpleName();
        switch (a){
            case "OrExpression":
                ArrayList<Expression> temp=new ArrayList<>();
                orExpressionList.add(temp);
                execute(((OrExpression) expression).getLeftExpression(),temp);
                execute(((OrExpression) expression).getRightExpression(),list);
                break;
            case "AndExpression":
                execute(((AndExpression) expression).getLeftExpression(),list);
                execute(((AndExpression) expression).getRightExpression(),list);
                break;
            default:
                list.add(expression); break;
        }
    }

    private SelectResult function(Function expression, SelectResult selectResult)  {
        String name = expression.getName();
        switch(name.toLowerCase()){
            case "st_within": return range(expression,selectResult);
            case "st_intersect": return path(expression,selectResult);
            case "st_contain": return strictPath(expression,selectResult);
            case "st_similarity": return topK(expression,selectResult);
        }
        return null;

    }

    private SelectResult topK(Function expression, SelectResult selectResult)  {
        List<Expression> expressions = expression.getParameters().getExpressions();

        List<Expression> list = ((Function) expressions.get(0)).getParameters().getExpressions();
        Trajectory<TrajEntry> trajEntries = new Trajectory<>();
        for (int i = 0; i < list.size(); i+=2) {
            Double lat = Double.parseDouble(list.get(i+1).toString());
            Double lng = Double.parseDouble(list.get(i).toString());
            Coordinate coordinate = new Coordinate(lat, lng);
            trajEntries.add(coordinate);
        }
        int k=Integer.parseInt(expressions.get(1).toString());
        List<Trajectory<TrajEntry>> trajectories=new ArrayList<>();
        // two parameter without the similarity function
        if(expressions.size()==2) {
            trajectories = TorchConnect.getTorchConnect().topkQuery(trajEntries, k);
        }
        //three parameter contains the similarity function
        else{
            String similarityFunction=((StringValue)expressions.get(2)).getValue();
            trajectories = TorchConnect.getTorchConnect().topkQuery(trajEntries, k,similarityFunction);
        }
        ArrayList<String> trajIds=new ArrayList<>();
        for (Trajectory t :
               trajectories ) {
            trajIds.add(t.id);
        }
        TorchConnect.getTorchConnect().updateTrajMap(trajIds);
        return TrajTrans.getSelectResultByTrajList(trajectories,selectResult);
    }

    private SelectResult strictPath(Function expression, SelectResult selectResult) {
        List<Expression> expressions = expression.getParameters().getExpressions();
        Expression para = expressions.get(0);
        if(para.getClass().getSimpleName().toLowerCase().equals("function")){
            List<Expression> list = ((Function) para).getParameters().getExpressions();
            Trajectory<TrajEntry> trajEntries = new Trajectory<>();
            for (int i = 0; i < list.size(); i+=2) {
                Double lat = Double.parseDouble(list.get(i+1).toString());
                Double lng = Double.parseDouble(list.get(i).toString());
                Coordinate coordinate = new Coordinate(lat, lng);
                trajEntries.add(coordinate);
            }
            List<Trajectory<TrajEntry>> trajectories = TorchConnect.getTorchConnect().strictPathQuery(trajEntries);
            return TrajTrans.getSelectResultByTrajList(trajectories,selectResult);
        }
        else{
            List<Trajectory<TrajEntry>> trajectories = TorchConnect.getTorchConnect().strictPathQuery(para.toString());
            return TrajTrans.getSelectResultByTrajList(trajectories,selectResult);

        }
    }

    private SelectResult path(Function expression, SelectResult selectResult) {
        List<Expression> expressions = expression.getParameters().getExpressions();
        Expression para = expressions.get(0);
        if(para.getClass().getSimpleName().toLowerCase().equals("function")){
            List<Expression> list = ((Function) para).getParameters().getExpressions();
            Trajectory<TrajEntry> trajEntries = new Trajectory<>();
            for (int i = 0; i < list.size(); i+=2) {
                Double lat = Double.parseDouble(list.get(i+1).toString());
                Double lng = Double.parseDouble(list.get(i).toString());
                Coordinate coordinate = new Coordinate(lat, lng);
                trajEntries.add(coordinate);
            }
            List<Trajectory<TrajEntry>> trajectories = TorchConnect.getTorchConnect().pathQuery(trajEntries);
            return TrajTrans.getSelectResultByTrajList(trajectories,selectResult);
        }
        else{
            List<Trajectory<TrajEntry>> trajectories = TorchConnect.getTorchConnect().pathQuery(para.toString());
            return TrajTrans.getSelectResultByTrajList(trajectories,selectResult);

        }
    }

    private SelectResult range(Function expression, SelectResult selectResult) {
        List<Expression> expressions = expression.getParameters().getExpressions();
        Function searchWindow = (Function) expressions.get(0);
        List<Expression> swPara = searchWindow.getParameters().getExpressions();
        if(swPara.get(1).getClass().getSimpleName().toLowerCase().equals("function")){
            List<Expression> coor1 = ((Function) swPara.get(0)).getParameters().getExpressions();
            List<Expression> coor2 = ((Function) swPara.get(1)).getParameters().getExpressions();
            Double lat1=Double.parseDouble(coor1.get(1).toString());
            Double lng1=Double.parseDouble(coor1.get(0).toString());
            Coordinate coordinate1 = new Coordinate(lat1, lng1);
            Double lat2=Double.parseDouble(coor2.get(1).toString());
            Double lng2=Double.parseDouble(coor2.get(0).toString());
            Coordinate coordinate2 = new Coordinate(lat2, lng2);
            TorchConnect torchConnect = TorchConnect.getTorchConnect();
            SearchWindow searchWindow1 = new SearchWindow(coordinate1, coordinate2);
            List<Trajectory<TrajEntry>> trajectories = torchConnect.rangeQuery(searchWindow1);
            return TrajTrans.getSelectResultByTrajList(trajectories,selectResult);
        }
        TorchConnect torchConnect = TorchConnect.getTorchConnect();
        Function coordinate = (Function) swPara.get(0);
        List<Expression> coor = coordinate.getParameters().getExpressions();
        Double lat=Double.parseDouble(coor.get(1).toString());
        Double lng=Double.parseDouble(coor.get(0).toString());
        double radius = Double.parseDouble(swPara.get(1).toString());
        Coordinate coordinate1 = new Coordinate(lat, lng);
        List<Trajectory<TrajEntry>> trajectories = torchConnect.rangeQuery(new SearchWindow(coordinate1, radius));
        return TrajTrans.getSelectResultByTrajList(trajectories,selectResult);
    }



    public SelectResult andExpression(AndExpression expression, SelectResult selectResult) throws TMDBException, IOException {
        Expression left=expression.getLeftExpression();
        Expression right=expression.getRightExpression();
        SelectResult selectResult1=execute(left,selectResult);
        SelectResult selectResult2=execute(right,selectResult);
        HashSet<Tuple> selectResultSet1=getTupleSet(selectResult1);
        HashSet<Tuple> selectResultSet2=getTupleSet(selectResult2);
        HashSet<Tuple> overlap=new HashSet<>();
        //将两个条件都满足的Tuple加入overlap中
        for(Tuple tuple:selectResultSet2){
            if(selectResultSet1.contains(tuple)) overlap.add(tuple);
        }
        return getSelectResultFromSet(selectResult,overlap);
    }

    public SelectResult or(SelectResult selectResult1,SelectResult selectResult2) throws TMDBException, IOException {
        HashSet<Tuple> selectResultSet1=getTupleSet(selectResult1);
        HashSet<Tuple> selectResultSet2=getTupleSet(selectResult2);
        //将selectResultSet2中tuple加入selectResultSet1中，这里将selectResultSet1作为结果集合
        for(Tuple tuple:selectResultSet2){
            selectResultSet1.add(tuple);
        }
        return getSelectResultFromSet(selectResult1,selectResultSet1);
    }

    public SelectResult inExpression(InExpression expression, SelectResult selectResult) throws TMDBException, IOException {
        ArrayList<Object> left=formula.formulaExecute(expression.getLeftExpression(),selectResult);
        List<Object> right=new ArrayList<>();
        //in表达式右边可能是一个list
        if(expression.getRightItemsList()!=null){
            for(Expression expression1:((ExpressionList)expression.getRightItemsList()).getExpressions()){
                ArrayList<Object> temp2=formula.formulaExecute(expression1,selectResult);
                right.add(transType(temp2.get(0)));
            }
        }
        //in表达式的右边可能是一个SubSelect
        else if(expression.getRightExpression().getClass().getSimpleName().equals("SubSelect")){
            Select select=new SelectImpl();
            SelectResult temp=select.select(expression.getRightExpression());
            for(int i=0;i<temp.getTpl().tuplelist.size();i++){
                right.add(transType(temp.getTpl().tuplelist.get(i).tuple[0]));
            }
        }
        ArrayList<Tuple> resTuple=new ArrayList<>();
        //最后，如果left存在于right的集合中，就加入到结果集合
        for(int i=0;i<left.size();i++){
            if(right.contains(transType(left.get(i)))) resTuple.add(selectResult.getTpl().tuplelist.get(i));
        }
        selectResult.getTpl().tuplelist=resTuple;
        return selectResult;
    }

    public SelectResult equalsToExpression(EqualsTo expression,SelectResult selectResult) throws TMDBException {
        ArrayList<Object> left=formula.formulaExecute(expression.getLeftExpression(),selectResult);
        ArrayList<Object> right=formula.formulaExecute(expression.getRightExpression(),selectResult);
        HashSet<Tuple> set=new HashSet<>();
        for(int i=0;i<left.size();i++){
            String tempLeft=transType(left.get(i));
            String tempRight=transType(right.get(i));
            //左边和右边相等则加入结果集合。
            if(tempLeft.equals(tempRight)) set.add(selectResult.getTpl().tuplelist.get(i));
        }
        return getSelectResultFromSet(selectResult,set);
    }

    public SelectResult minorThan(MinorThan expression,SelectResult selectResult) throws TMDBException {
        ArrayList<Object> left=formula.formulaExecute(expression.getLeftExpression(),selectResult);
        ArrayList<Object> right=formula.formulaExecute(expression.getRightExpression(),selectResult);
        HashSet<Tuple> set=new HashSet<>();
        for(int i=0;i<left.size();i++){
            String tempLeft=transType(left.get(i));
            String tempRight=transType(right.get(i));
            //左边小于右边，则加入结果集合
            if(tempLeft.compareTo(tempRight)<0) set.add(selectResult.getTpl().tuplelist.get(i));
        }
        return getSelectResultFromSet(selectResult,set);
    }

    public SelectResult greaterThan(GreaterThan expression,SelectResult selectResult) throws TMDBException {
        ArrayList<Object> left=formula.formulaExecute(expression.getLeftExpression(),selectResult);
        ArrayList<Object> right=formula.formulaExecute(expression.getRightExpression(),selectResult);
        HashSet<Tuple> set=new HashSet<>();
        for(int i=0;i<left.size();i++){
            String tempLeft=transType(left.get(i));
            String tempRight=transType(right.get(i));
            //左边大于右边，则加入结果集合
            if(tempLeft.compareTo(tempRight)>0) set.add(selectResult.getTpl().tuplelist.get(i));
        }
        return getSelectResultFromSet(selectResult,set);
    }
    

    public HashSet<Tuple> getTupleSet(SelectResult selectResult){
        HashSet<Tuple> set=new HashSet<>();
        for(Tuple tuple:selectResult.getTpl().tuplelist){
            set.add(tuple);
        }
        return set;
    }

    public SelectResult getSelectResultFromSet(SelectResult selectResult,HashSet<Tuple> set){
        TupleList tupleList=new TupleList();
        for(Tuple tuple:set){
            tupleList.addTuple(tuple);
        }
        selectResult.setTpl(tupleList);
        return selectResult;
    }

    //进行类型转换，很多时候需要使用
    public String transType(Object obj){
        switch(obj.getClass().getSimpleName()){
            case "String":
                boolean flag=false;
                try{
                    Double temp=Double.parseDouble(String.valueOf(obj));
                    flag=true;
                }
                catch(Throwable throwable){}
                if(flag==true) return String.valueOf(Double.parseDouble(String.valueOf(obj)));
                else return (String)obj;
            case "Float": return String.valueOf((double) obj);
            case "Double": return String.valueOf(obj);
            case "Integer": return String.valueOf((double) obj);
            case "Long": return String.valueOf((long) obj);
            case "Character": return String.valueOf(obj);
            case "Short": return String.valueOf((double) obj);
            default: return "";
        }
    }

}
