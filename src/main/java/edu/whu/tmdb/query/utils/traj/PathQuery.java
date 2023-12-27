package edu.whu.tmdb.query.utils.traj;/*
 * className:PathQuery
 * Package:edu.whu.tmdb.query.utils.traj
 * Description:
 * @Author: xyl
 * @Create:2023/12/11 - 20:15
 * @Version:v1
 */

import au.edu.rmit.bdm.Torch.base.model.Coordinate;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.base.model.Trajectory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class PathQuery implements Callable<Boolean> {
    public List<TrajEntry> path;
    public List<TrajEntry> traj;

    public PathQuery(List<TrajEntry> path, List<TrajEntry> traj) {
        this.path = path;
        this.traj = traj;
    }

    @Override
    public Boolean call() throws Exception {
        return queryPaths(path,traj);
    }

    // 检查两条轨迹是否存在边的重叠
    public boolean queryPaths(List<TrajEntry> path, List<TrajEntry> traj) {
        for (int i = 0; i < path.size() - 1; i++) {
            Coordinate segmentStartTraj = (Coordinate) path.get(i);
            Coordinate segmentEndTraj = (Coordinate) path.get(i + 1);

            for (int j = 0; j < traj.size() - 1; j++) {
                Coordinate segmentStartPath = (Coordinate) traj.get(j);
                Coordinate segmentEndPath = (Coordinate) traj.get(j + 1);

                if (linesIntersect(segmentStartTraj, segmentEndTraj, segmentStartPath, segmentEndPath)) {
                    return true;
                }
            }
        }
        return false;
    }

    boolean linesIntersect(Coordinate p1, Coordinate p2, Coordinate q1, Coordinate q2) {
        return ccw(p1, q1, q2) != ccw(p2, q1, q2) && ccw(p1, p2, q1) != ccw(p1, p2, q2);
    }

    boolean ccw(Coordinate a, Coordinate b, Coordinate c) {
        return (c.getLng() - a.getLng()) * (b.getLat() - a.getLat()) > (b.getLng() - a.getLng()) * (c.getLat() - a.getLat());
    }
}
