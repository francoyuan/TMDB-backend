package edu.whu.tmdb.gtfs;/*
 * className:TrajectorySimplifier
 * Package:edu.whu.tmdb.gtfs
 * Description:
 * @Author: xyl
 * @Create:2024/1/12 - 14:02
 * @Version:v1
 */

import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import pl.luwi.series.reducer.Point;
import pl.luwi.series.reducer.SeriesReducer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class TrajectorySimplifier {
    public static void simplify() throws IOException {
        String filePath = "/Users/woshi/Project/IdeaProjects/TMDB/data/Gtfs.txt";
        String outputPath = "/Users/woshi/Project/IdeaProjects/TMDB/data/Gtfs_simplified.txt";
        File file = new File(filePath);
        File outputFile = new File(outputPath);
        if (!file.exists()) {
            System.out.println("File not exists!");
            return;
        }
        // 我想要读取/Users/woshi/Project/IdeaProjects/TMDB/data/Qtfs.txt这个文件点内容，这个文件有两列，中间用空格分隔，我只想要第二列的内容
        //然后这个是一个轨迹点组成点序列。基数点代表纬度，偶数点代表经度。我想要提取成一个轨迹点list
        //然后调用SeriesReducer.reduce(points,epsilon)方法简化这个轨迹list，然后再写入文件
        //这个文件的格式是：轨迹点序号 纬度，经度，纬度，经度.....
        //开始读取文件
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
            String line;
            while ((line = br.readLine()) != null) {
                String[] strs = line.split(" ");
                if (strs.length < 2) continue; // 跳过格式不正确的行

                String[] strs1 = strs[1].split(",");
                List<Point> points = new LinkedList<>();
                for (int i = 0; i < strs1.length - 1; i += 2) {
                    Point point = new MyPoint(Double.parseDouble(strs1[i]), Double.parseDouble(strs1[i + 1]));
                    points.add(point);
                }
                List<Point> reduce = SeriesReducer.reduce(points, 0.0001);

                String simpleLine = strs[0] + " ";
                for (int i = 0; i < reduce.size(); i++) {
                    simpleLine += reduce.get(i).getX() + "," + reduce.get(i).getY();
                    if (i < reduce.size() - 1) simpleLine += ",";
                }
                bw.write(simpleLine);
                bw.newLine();
            }
            br.close();
            bw.close();
        } catch (FileNotFoundException e) {
            System.out.println("File not found: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO exception: " + e.getMessage());
        }

    }
}

class MyPoint implements Point {
    private double x;
    private double y;

    public MyPoint(double x, double y) {
        this.x = x;
        this.y = y;
    }


    @Override
    public double getX() {
        return this.x;
    }

    @Override
    public double getY() {
        return this.y;

    }
}