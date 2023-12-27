package edu.whu.tmdb.query.utils.traj;/*
 * className:TrajQuery
 * Package:edu.whu.tmdb.query.utils
 * Description:
 * @Author: xyl
 * @Create:2023/12/7 - 10:23
 * @Version:v1
 */

import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.base.model.Trajectory;
import au.edu.rmit.bdm.Torch.queryEngine.model.SearchWindow;

import java.util.List;
import java.util.concurrent.Callable;

public class RangeQuery implements Callable<Boolean> {

    private SearchWindow searchWindow;
    private List<TrajEntry> t;


    public RangeQuery(SearchWindow searchWindow, List<TrajEntry> t) {
        this.searchWindow = searchWindow;
        this.t = t;
    }

    @Override
    public Boolean call() throws Exception {
        SearchWindow mbr = TrajUtil.getMBR(t);
        if(mbr==null){
            return false;
        }
        return SpatialUtil.isOverlapping(mbr,searchWindow);
    }

}
