package edu.whu.tmdb.query.utils.traj;/*
 * className:SpatialUtil
 * Package:edu.whu.tmdb.query.utils.traj
 * Description:
 * @Author: xyl
 * @Create:2023/12/7 - 10:56
 * @Version:v1
 */

import au.edu.rmit.bdm.Torch.queryEngine.model.SearchWindow;

public class SpatialUtil {
    public static boolean isOverlapping(SearchWindow sw1, SearchWindow sw2) {
        // 检查一个矩形是否在另一个矩形的左侧或右侧
        boolean isHorizontallySeparate = sw1.rightLng < sw2.leftLng || sw2.rightLng < sw1.leftLng;

        // 检查一个矩形是否在另一个矩形的上方或下方
        boolean isVerticallySeparate = sw1.lowerLat > sw2.upperLat || sw2.lowerLat > sw1.upperLat;

        // 如果两个矩形在水平或垂直方向上是分开的，则它们不重叠
        return !(isHorizontallySeparate || isVerticallySeparate);
    }
}
