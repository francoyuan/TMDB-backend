package edu.whu.tmdb.query.utils.traj;/*
 * className:SpatialTrans
 * Package:edu.whu.tmdb.query.utils.traj
 * Description:
 * @Author: xyl
 * @Create:2023/12/3 - 15:53
 * @Version:v1
 */

import au.edu.rmit.bdm.Torch.base.model.Coordinate;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.base.model.Trajectory;
import au.edu.rmit.bdm.Torch.queryEngine.model.SearchWindow;
import edu.whu.tmdb.query.torch.TorchConnect;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.List;

public class SpatialTrans {
    public static SearchWindow getSearchWindow(Function expression){
        List<Expression> expressions = expression.getParameters().getExpressions();
        net.sf.jsqlparser.expression.Function searchWindow = (net.sf.jsqlparser.expression.Function) expressions.get(0);
        List<Expression> swPara = searchWindow.getParameters().getExpressions();
        if(swPara.get(1).getClass().getSimpleName().toLowerCase().equals("function")) {
            List<Expression> coor1 = ((net.sf.jsqlparser.expression.Function) swPara.get(0)).getParameters().getExpressions();
            List<Expression> coor2 = ((net.sf.jsqlparser.expression.Function) swPara.get(1)).getParameters().getExpressions();
            Double lat1 = Double.parseDouble(coor1.get(0).toString());
            Double lng1 = Double.parseDouble(coor1.get(1).toString());
            Coordinate coordinate1 = new Coordinate(lat1, lng1);
            Double lat2 = Double.parseDouble(coor2.get(0).toString());
            Double lng2 = Double.parseDouble(coor2.get(1).toString());
            Coordinate coordinate2 = new Coordinate(lat2, lng2);
            return new SearchWindow(coordinate1, coordinate2);
        }
        net.sf.jsqlparser.expression.Function coordinate = (net.sf.jsqlparser.expression.Function) swPara.get(0);
        List<Expression> coor = coordinate.getParameters().getExpressions();
        Double lat=Double.parseDouble(coor.get(0).toString());
        Double lng=Double.parseDouble(coor.get(1).toString());
        double radius = Double.parseDouble(swPara.get(1).toString());
        Coordinate coordinate1 = new Coordinate(lat, lng);
        return new SearchWindow(coordinate1, radius);
    }

    public static Trajectory<TrajEntry> getTrajectory(Function expression){
        List<Expression> expressions = expression.getParameters().getExpressions();
        Expression para = expressions.get(0);
        Trajectory<TrajEntry> trajectory = new Trajectory<>();
        if(para.getClass().getSimpleName().toLowerCase().equals("function")){
            List<Expression> list = ((Function) para).getParameters().getExpressions();
            for (int i = 0; i < list.size(); i+=2) {
                Double lat = Double.parseDouble(list.get(i+1).toString());
                Double lng = Double.parseDouble(list.get(i).toString());
                Coordinate coordinate = new Coordinate(lat, lng);
                trajectory.add(coordinate);
            }
        }
        return trajectory;
    }
}
