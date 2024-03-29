package au.edu.rmit.bdm.Torch.queryEngine.query;

import au.edu.rmit.bdm.Torch.base.FileSetting;
import au.edu.rmit.bdm.Torch.base.Torch;
import au.edu.rmit.bdm.Torch.base.db.TrajEdgeRepresentationPool;
import au.edu.rmit.bdm.Torch.base.db.TrajVertexRepresentationPool;
import au.edu.rmit.bdm.Torch.base.db.TrajectoryPool;
import au.edu.rmit.bdm.Torch.base.helper.GeoUtil;
import au.edu.rmit.bdm.Torch.base.model.Coordinate;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.base.model.Trajectory;
import au.edu.rmit.bdm.Torch.queryEngine.model.TimeInterval;
import au.edu.rmit.bdm.Torch.queryEngine.model.TorchDate;

import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.utils.SelectResult;
import edu.whu.tmdb.storage.memory.Tuple;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static edu.whu.tmdb.util.FileOperation.getFileNameWithoutExtension;

public class TrajectoryResolver {

    private Logger logger = LoggerFactory.getLogger(TrajectoryResolver.class);
    private TrajEdgeRepresentationPool trajectoryPool;
    private TrajVertexRepresentationPool trajVertexRepresentationPool;
    private Map<Integer, String[]> rawEdgeLookup;
    private Map<String, TimeInterval> timeSpanLookup;
    private Map<Integer, Coordinate> vertexLookup;
    private boolean resolveAll;
    public FileSetting setting;
    public TimeInterval querySpan;
    public boolean contain;
    public boolean isNantong;

    public TrajectoryResolver( TrajEdgeRepresentationPool trajectoryPool, Map<Integer, String[]> rawEdgeLookup, boolean resolveAll){
        this.trajectoryPool = trajectoryPool;
        this.rawEdgeLookup = rawEdgeLookup;
        this.resolveAll = resolveAll;
    }

    public TrajectoryResolver(boolean resolveAll, boolean isNantong, FileSetting setting,boolean isMem)  {
        this.resolveAll = resolveAll;
        this.setting = setting;
        this.isNantong = isNantong;

        if (!isNantong) {
            trajectoryPool = new TrajEdgeRepresentationPool(isMem, setting);
            rawEdgeLookup = new HashMap<>();
            timeSpanLookup = new HashMap<>();
            loadRawEdgeLookupTable();
            loadTimeSpanLookupTable();
        }else{
            vertexLookup = new HashMap<>();
            trajVertexRepresentationPool = new TrajVertexRepresentationPool(isMem, setting);
            loadVertexLookup();
        }

    }

    public TrajectoryResolver(boolean resolveAll, boolean isNantong, FileSetting setting)  {
        this.resolveAll = resolveAll;
        this.setting = setting;
        this.isNantong = isNantong;

        if (!isNantong) {
            trajectoryPool = new TrajEdgeRepresentationPool(false, setting);
            rawEdgeLookup = new HashMap<>();
            timeSpanLookup = new HashMap<>();
//            loadRawEdgeLookupTable();
//            loadTimeSpanLookupTable();
        }else{
            vertexLookup = new HashMap<>();
            trajVertexRepresentationPool = new TrajVertexRepresentationPool(false, setting);
            loadVertexLookup();
        }

    }


    //todo 这个位置修改读id_vertex
    private void loadVertexLookup()  {
        String idVertex = getFileNameWithoutExtension(setting.ID_VERTEX_LOOKUP);
        PlainSelect plainSelect = new PlainSelect().withFromItem(new Table(idVertex));
        plainSelect.addSelectItems(new AllColumns());
        EqualsTo where = new EqualsTo(new Column().withColumnName("traj_name"), new StringValue(getFileNameWithoutExtension(setting.TorchBase)));
        plainSelect.setWhere(where);
        SelectResult result = Transaction.getInstance().query(new Select().withSelectBody(plainSelect));

        for (Tuple tuple:
                result.getTpl().tuplelist) {
            String[] splits = (String[]) tuple.tuple;
            vertexLookup.put(Integer.parseInt(splits[1]), new Coordinate(Double.parseDouble(splits[2]), Double.parseDouble(splits[3])));
        }

    }

    QueryResult resolve (String queryType, List<String> trajIds, List<TrajEntry> rawQuery, Trajectory<TrajEntry> _mappedQuery)  {

        List<TrajEntry> mappedQuery = _mappedQuery;
        if (!queryType.equals(Torch.QueryType.RangeQ))
            mappedQuery = resolveMappedQuery(_mappedQuery);

        logger.info("number of ids before: {}", trajIds.size());

        if (querySpan != null && !queryType.equals(Torch.QueryType.TopK)) {
            Iterator<String> iter = trajIds.iterator();
            if (contain) {
                while (iter.hasNext()) {
                    String id = iter.next();
                    TimeInterval candidate_time_span = timeSpanLookup.get(id);
                    if (!querySpan.contains(candidate_time_span))
                        iter.remove();
                }
            } else {  //join but not contain
                while (iter.hasNext()) {
                    String id = iter.next();
                    TimeInterval candidate_time_span = timeSpanLookup.get(id);
                    if (!querySpan.joins(candidate_time_span))
                        iter.remove();
                }
            }

        }

        logger.info("number of ids after: {}", trajIds.size());

        QueryResult ret;
        if (resolveAll)
            ret = QueryResult.genResolvedRet(queryType, resolveRet(trajIds), rawQuery, mappedQuery);
        else {
            int[] ids = new int[trajIds.size()];
            for (int i = 0; i < trajIds.size(); i++)
                ids[i] = Integer.valueOf(trajIds.get(i));
            ret = QueryResult.genUnresolvedRet(queryType, ids, rawQuery, mappedQuery);
        }
        return ret;
    }

    public boolean meetTimeConstrain(String trajId){
        if (querySpan == null) return true;

        if (contain)
            return querySpan.contains(timeSpanLookup.get(trajId));

        return querySpan.joins(timeSpanLookup.get(trajId));
    }

    private List<TrajEntry> resolveMappedQuery(Trajectory<TrajEntry> mappedQuery) {

        List<TrajEntry> l = new Trajectory<>();
        int queryLen = mappedQuery.edges.size();

        for (int i = 1; i < queryLen; i++) {

            String[] tokens = rawEdgeLookup.get(mappedQuery.edges.get(i).id);
            String[] lats = tokens[0].split(",");
            String[] lngs = tokens[1].split(",");

            for (int j = 0; j < lats.length; j++) {
                l.add(new Coordinate(Double.parseDouble(lats[j]),Double.parseDouble(lngs[j])));
            }
        }
        return l;
    }




    private List<Trajectory<TrajEntry>> resolveRet(Collection<String> trajIds)  {

        List<Trajectory<TrajEntry>> ret = null;

        if (!isNantong) {
            String[] tokens = null;

            ret = new ArrayList<>(trajIds.size());
            for (String trajId : trajIds) {

                int[] edges = trajectoryPool.get(trajId);
                if (edges == null) {
                    logger.debug("cannot find trajectory id {}, this should not be happened", trajId);
                    continue;
                }

                Trajectory<TrajEntry> t = new Trajectory<>();
                t.id = trajId;

                for (int i = 1; i < edges.length; i++) {

                    tokens = rawEdgeLookup.get(edges[i]);
                    String[] lats = tokens[0].split(",");
                    String[] lngs = tokens[1].split(",");

                    for (int j = 0; j < lats.length; j++) {
                        try {
                            t.add(new Coordinate(Double.parseDouble(lats[j]), Double.parseDouble(lngs[j])));
                        } catch (Exception ignored) {
                        }
                    }
                }
                ret.add(t);
            }
        }
        return ret;
    }

    private void loadRawEdgeLookupTable()  {

        logger.info("load raw edge lookup table");

        String idEdgeRaw = getFileNameWithoutExtension(setting.ID_EDGE_RAW);
        PlainSelect plainSelect = new PlainSelect().withFromItem(new Table(idEdgeRaw));
        plainSelect.addSelectItems(new AllColumns());
        EqualsTo where = new EqualsTo(new Column().withColumnName("traj_name"), new StringValue(getFileNameWithoutExtension(setting.TorchBase)));
        plainSelect.setWhere(where);
        SelectResult result = Transaction.getInstance().query(new Select().withSelectBody(plainSelect));

        for (Tuple tuple :
                result.getTpl().tuplelist) {
//            String[] tokens = (String[]) tuple.tuple;
            int id = ((Long)tuple.tuple[1]).intValue();
            String lats = String.valueOf(tuple.tuple[2]);
            String lngs = String.valueOf(tuple.tuple[3]);

            rawEdgeLookup.put(id, new String[]{lats, lngs});
        }
    }

    //todo 这里改time partial的信息
    private void loadTimeSpanLookupTable()  {

        logger.info("load time querySpan lookup table");

        String time = getFileNameWithoutExtension(setting.TRAJECTORY_START_END_TIME_PARTIAL);
        PlainSelect plainSelect = new PlainSelect().withFromItem(new Table(time));
        plainSelect.addSelectItems(new AllColumns());
        EqualsTo where = new EqualsTo(new Column().withColumnName("traj_name"), new StringValue(getFileNameWithoutExtension(setting.TorchBase)));
        plainSelect.setWhere(where);
        SelectResult result = Transaction.getInstance().query(new Select().withSelectBody(plainSelect));

        for (Tuple tuple :
                result.getTpl().tuplelist) {
//            String[] c = (String[]) tuple.tuple;
            timeSpanLookup.put(String.valueOf(tuple.tuple[1]), buildInterval(String.valueOf(tuple.tuple[1]),
                    (String) tuple.tuple[2],
                    (String) tuple.tuple[3]));
        }

    }

    private TimeInterval buildInterval(String id, String start, String end) {
        TorchDate startDate = new TorchDate().setAll(start);
        TorchDate endDate = new TorchDate().setAll(end);
        return new TimeInterval(id, startDate, endDate);
    }

    public void setTimeInterval(TimeInterval span, boolean contain) {
        this.querySpan = span;
        this.contain = contain;
    }

    public TrajectoryPool getTrajectoryPool() {
        return trajectoryPool;
    }

    public void setTrajectoryPool(TrajEdgeRepresentationPool trajectoryPool) {
        this.trajectoryPool = trajectoryPool;
    }
}
