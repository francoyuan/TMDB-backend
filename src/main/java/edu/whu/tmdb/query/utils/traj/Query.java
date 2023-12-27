package edu.whu.tmdb.query.utils.traj;/*
 * className:Query
 * Package:edu.whu.tmdb.query.utils.traj
 * Description:
 * @Author: xyl
 * @Create:2023/12/7 - 11:07
 * @Version:v1
 */

import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.base.model.Trajectory;
import au.edu.rmit.bdm.Torch.queryEngine.model.SearchWindow;
import org.checkerframework.checker.units.qual.A;
import org.glassfish.jersey.message.internal.ParameterizedHeader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public class Query {
    public static List<List<TrajEntry>> rangeQuery(SearchWindow searchWindow, List<List<TrajEntry>> trajs){
        ThreadPoolExecutor executor = getExecutor();
        List<List<TrajEntry>> res=new CopyOnWriteArrayList<>();
        for (List<TrajEntry> t :
                trajs) {
            Future<Boolean> future = executor.submit(new RangeQuery(searchWindow,t));
            try {
                if (future.get()){
                    res.add(t);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        executor.shutdown(); // 关闭线程池，不再接受新任务
        try {
            // 等待指定的时间让线程池中的任务全部执行完毕
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                // 如果超时，则强制关闭尚未完成的任务
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            // 如果等待被中断，也立即尝试关闭线程池
            executor.shutdownNow();
            Thread.currentThread().interrupt(); // 保留中断状态
        }
        return res;
    }

    public static List<List<TrajEntry>> pathQuery(List<TrajEntry> path,List<List<TrajEntry>> trajs){
        ThreadPoolExecutor executor = getExecutor();
        List<List<TrajEntry>> res=new CopyOnWriteArrayList<>();
        for (List<TrajEntry> t :
                trajs) {
            Future<Boolean> future = executor.submit(new PathQuery(path,t));
            try {
                if (future.get()){
                    res.add(t);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        executor.shutdown(); // 关闭线程池，不再接受新任务

        try {
            // 等待指定的时间让线程池中的任务全部执行完毕
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                // 如果超时，则强制关闭尚未完成的任务
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            // 如果等待被中断，也立即尝试关闭线程池
            executor.shutdownNow();
            Thread.currentThread().interrupt(); // 保留中断状态
        }
        return res;
    }

    public static ThreadPoolExecutor getExecutor(){
        return new ThreadPoolExecutor(
                3,
                10,
                10,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }
}
