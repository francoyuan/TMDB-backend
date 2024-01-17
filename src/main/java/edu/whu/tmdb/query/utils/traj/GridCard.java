package edu.whu.tmdb.query.utils.traj;/*
 * className:GridCard
 * Package:edu.whu.tmdb.query.utils
 * Description:
 * @Author: xyl
 * @Create:2023/12/2 - 15:27
 * @Version:v1
 */

import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.base.model.Trajectory;
import au.edu.rmit.bdm.Torch.queryEngine.model.SearchWindow;
import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.utils.KryoSerialization;
import edu.whu.tmdb.query.utils.SelectResult;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.utils.K;

import java.util.HashMap;
import java.util.Map;

public class GridCard {
    private HashMap<String,Integer> gridCard;
    private HashMap<String,Integer> gridVertex;
    private HashMap<String,Integer> gridTrajV;
    private float left,right,upper,lower,deltaLon,deltaLat;
    
    private int horizontalTileNumber,verticalTileNumber;
    private String base;

    public GridCard init(String base){
        this.base=base;
        try {
            gridCard = (HashMap<String, Integer>) KryoSerialization.deserializeFromString(
                    MemManager.getInstance().search(
                            new K(base + "/gridCard")).valueString);
            gridVertex = (HashMap<String, Integer>) KryoSerialization.deserializeFromString(
                    MemManager.getInstance().search(
                            new K(base + "/gridVertex")).valueString);
            gridTrajV = (HashMap<String, Integer>) KryoSerialization.deserializeFromString(
                    MemManager.getInstance().search(
                            new K(base + "/gridTrajV")).valueString);
            HashMap<String, Float> gridInfo = (HashMap<String, Float>) KryoSerialization.deserializeFromString(
                    MemManager.getInstance().search(
                            new K(base + "/gridInfo")).valueString);
            if (gridCard == null || gridInfo == null) {
                return null;
            }
            this.deltaLat = gridInfo.get("deltaLat");
            this.deltaLon = gridInfo.get("deltaLon");
            this.upper = gridInfo.get("upper");
            this.lower = gridInfo.get("lower");
            this.left = gridInfo.get("left");
            this.right = gridInfo.get("right");
            this.horizontalTileNumber = gridInfo.get("horizontalTileNumber").intValue();
            this.verticalTileNumber = gridInfo.get("verticalTileNumber").intValue();
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
        return this;
    }

    
    private int calculateTileID(double lat, double lon) {
        int row = (int) Math.floor((upper - lat) / deltaLat);
        int col = (int) Math.ceil((lon - left) / deltaLon);
        int tileID = row * horizontalTileNumber + col;
        if (tileID < 0) {
            return 0;
        }
        if (tileID > horizontalTileNumber * verticalTileNumber) {
            return horizontalTileNumber * verticalTileNumber;
        }
        return tileID;
    }

    public int getRangeCard(SearchWindow searchWindow){
        int upperLeft = calculateTileID(searchWindow.upperLat, searchWindow.leftLng);
        int lowerRight = calculateTileID(searchWindow.lowerLat, searchWindow.rightLng);
        int topLeftRow = upperLeft / horizontalTileNumber;
        int topLeftCol = upperLeft % horizontalTileNumber;
        int bottomRightRow = lowerRight / horizontalTileNumber;
        int bottomRightCol = lowerRight % horizontalTileNumber;
        int count=0;
        for (int row = topLeftRow; row <= bottomRightRow; row++) {
            for (int col = topLeftCol; col <= bottomRightCol; col++) {
                int gridNumber = row * horizontalTileNumber + col;
                count+=gridCard.getOrDefault(gridNumber,0);
            }
        }
        int res= (int) Math.round(count*calculateOverrideRate());
        return res;
    }

    public int getPathCard(Trajectory<TrajEntry> path) {
        Map<Integer, Integer> visitedGrids = new HashMap<>();
        double pathCard = 0;
        for (TrajEntry point : path) {
            int tileID = calculateTileID(point.getLat(), point.getLng());
            visitedGrids.put(tileID,visitedGrids.getOrDefault(tileID,0)+1);
        }
        for (int tileID:visitedGrids.keySet()) {
            int current = gridCard.getOrDefault(tileID, 0);
            if(current==0){
                continue;
            }
            double tRatio =(double)gridTrajV.get(tileID)/ (double)gridVertex.get(tileID);
            double pRatio =Math.pow((double)visitedGrids.get(tileID),2)/ (double)gridVertex.get(tileID);
            pathCard+=current*tRatio*pRatio;
        }
        return (int) Math.round(pathCard);
    }

    public int getStrictPathCard(Trajectory<TrajEntry> path) {
        Map<Integer, Integer> visitedGrids = new HashMap<>();
        double pathCard = 1;
        for (TrajEntry point : path) {
            int tileID = calculateTileID(point.getLat(), point.getLng());
            visitedGrids.put(tileID,visitedGrids.getOrDefault(tileID,0)+1);
        }
        for (int tileID:visitedGrids.keySet()) {
            int current = gridCard.getOrDefault(tileID, 0);
            if(current==0){
                continue;
            }
            double tRatio =(double)gridTrajV.get(tileID)/ (double)gridVertex.get(tileID);
            double pRatio =Math.pow((double)visitedGrids.get(tileID),2)/ (double)gridVertex.get(tileID);
            pathCard*=current*tRatio*pRatio;
        }
        return (int) Math.round(pathCard);
    }


    private double calculateOverallAverageDensity() {
        return (double)getTotalDensity() / (double)(horizontalTileNumber*verticalTileNumber);
    }

    private double calculateOverrideRate(){
        String sql="select * from traj" +
                " where traj_name='porto_raw_trajectory_full';";
        SelectResult query = Transaction.getInstance().query(sql);
        double res=(double)query.getTpl().tuplenum/(double)getTotalDensity();
        return res;
    }

    private int getTotalDensity(){
        int res= gridCard.values().stream().mapToInt(Integer::intValue).sum();
        return res;
    }
    
}
