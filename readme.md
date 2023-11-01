# TMDB-backend

## 对象代理

目前通过修改[JSqlPaser](https://github.com/francoyuan/JSqlParser)支持一下查询语法

+ 创建代理类

   ```create deputyclass xxxx as select xxx```

+ 跨类查询 

  ```select a->b, c from table```

（当前已经实现了代理类创建更新的相关逻辑)

## Torch轨迹查询

内部集成了torch用于轨迹查询。目前已经支持

+ 将map mathcing的数据和路网存储在tmdb中

+ 通过增加后端逻辑，支持使用Sql进行轨迹查询

  + Range query

  ```
  select traj_id, traj
  from traj
  where st_within(SearchWindow(Coordiante(50,50),50000000));//中心和半径
  
  select traj_id, traj
  from traj
  where st_within(SearchWindow(Coordiante(lng,lat),Coordiante(lng,lat)));//左上角和右下角
  ```

  + Path Query

  ```
  select traj_id, traj
  from traj
  where st_intersect(Trajectory(......));//具体具体的trajectory
  ```

  + TopK Similarity Query

  ```
  select traj_id,traj
  from traj
  where st_similarity(Trajectory(......), 3);
  
  select traj_id,traj
  from traj
  where st_similarity(Trajectory(......), k,similarityFunction) //DTW|H|LCSS|F|EDR
  
  ```

  