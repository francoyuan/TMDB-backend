# TMDB-backend

## 对象代理

目前通过修改[JSqlPaser](https://github.com/francoyuan/JSqlParser)支持一下查询语法

+ 创建代理类

   ```create deputyclass xxxx as select xxx```

+ 跨类查询 

  ```select a->b, c from table```

（当前已经实现了代理类创建更新的相关逻辑)

## Torch轨迹查询

内部集成并修改了[Torch](https://github.com/tgbnhy/torchtrajectory)用于轨迹查询。目前已经能够

+ 将map mathcing的数据和路网存储在tmdb中

+ 通过增加后端逻辑，支持使用Sql进行轨迹查询

  + Range query

  ```sql
  select traj_id, traj
  from traj
  where st_within(SearchWindow(Coordiante(50,50),50000000));//中心和半径
  
  select traj_id, traj
  from traj
  where st_within(SearchWindow(Coordiante(lng,lat),Coordiante(lng,lat)));//左上角和右下角
  ```

  + Path Query

  ```sql
  select traj_id, traj
  from traj
  where st_intersect(Trajectory(......));//具体具体的trajectory
  ```

  + TopK Similarity Query

  ```sql
  select traj_id,traj
  from traj
  where st_similarity(Trajectory(......), 3);
  
  select traj_id,traj
  from traj
  where st_similarity(Trajectory(......), k,similarityFunction) //DTW|H|LCSS|F|EDR
  ```

## Instruction

### Sql入口

在Transaction的query接受sql语句，进行解析以及后续的逻辑处理。另外，Transaction的query能够直接接收使用[JSqlParser](https://github.com/francoyuan/JSqlParser)构建的Sql Statement，能够跳过SQL解析步骤，直接进入逻辑处理。

```java
Transaction.getInstance().query("CREATE CLASS company (name char,age int, salary int);");
Transaction.getInstance().query(Statement statement);
```

### 轨迹查询

轨迹查询首先需要进行map matching，需要自行提供osm文件和轨迹文件，在data目录下有一个实例。调用mapMatching接口传入baseDir，轨迹原始文件地址，osm原始文件地址进行相关处理。

```
Transaction.getInstance().mapMathcing(baseDir, trajSrc, osmSrc);
```

轨迹的查询除了能够通过sql，也能调用engine，读取query txt进行查询，在data目录下有一个实例。

```
Transaction.getInstance().engine(baseDir, querySrc);
```

