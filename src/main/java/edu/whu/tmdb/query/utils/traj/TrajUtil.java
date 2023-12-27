package edu.whu.tmdb.query.utils.traj;/*
 * className:TrajUtil
 * Package:edu.whu.tmdb.query.utils.traj
 * Description:
 * @Author: xyl
 * @Create:2023/12/7 - 10:44
 * @Version:v1
 */

import au.edu.rmit.bdm.Torch.base.model.Coordinate;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.queryEngine.model.SearchWindow;

import java.awt.geom.Rectangle2D;
import java.util.List;

public class TrajUtil {
    public static SearchWindow getMBR(List<TrajEntry> trajectory) {
        if (trajectory == null || trajectory.isEmpty()) {
            System.out.println("Trajectory is null or empty. Skipping processing.");
            return null; // 直接返回，不进行进一步处理
        }

        double minX = Double.MAX_VALUE;
        double maxX = -Double.MAX_VALUE;
        double minY = Double.MAX_VALUE;
        double maxY = -Double.MAX_VALUE;

        for (TrajEntry entry : trajectory) {
            double lat = entry.getLat();
            double lng = entry.getLng();
            minX = Math.min(minX, lng);
            maxX = Math.max(maxX, lng);
            minY = Math.min(minY, lat);
            maxY = Math.max(maxY, lat);
        }

        TrajEntry upperLeft = new Coordinate(maxY, minX); // 最大纬度，最小经度
        TrajEntry lowerRight = new Coordinate(minY, maxX); // 最小纬度，最大经度

        return new SearchWindow(upperLeft, lowerRight);
    }








}
