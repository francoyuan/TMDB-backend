package au.edu.rmit.bdm.Torch.mapMatching.algorithm;

import au.edu.rmit.bdm.Torch.base.FileSetting;
import au.edu.rmit.bdm.Torch.base.Torch;
import au.edu.rmit.bdm.Torch.base.helper.GeoUtil;
import au.edu.rmit.bdm.Torch.base.helper.MemoryUsage;
import au.edu.rmit.bdm.Torch.base.model.TorEdge;
import au.edu.rmit.bdm.Torch.mapMatching.MMProperties;
import au.edu.rmit.bdm.Torch.mapMatching.MapMatching;
import au.edu.rmit.bdm.Torch.mapMatching.model.PillarVertex;
import au.edu.rmit.bdm.Torch.mapMatching.model.TorVertex;
import au.edu.rmit.bdm.Torch.mapMatching.model.TowerVertex;
import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.graphhopper.GraphHopper;
import com.graphhopper.GraphHopperConfig;
import com.graphhopper.config.Profile;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.routing.ev.BooleanEncodedValue;
import com.graphhopper.routing.ev.DecimalEncodedValue;
import com.graphhopper.routing.util.*;
import com.graphhopper.routing.weighting.Weighting;
import com.graphhopper.routing.weighting.custom.CustomWeighting;
import com.graphhopper.storage.Graph;
import com.graphhopper.storage.NodeAccess;
import com.graphhopper.util.FetchMode;
import com.graphhopper.util.PointList;

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

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static edu.whu.tmdb.util.FileOperation.getFileNameWithoutExtension;

/**
 * A torGraph stores all information required for trajectory projection.
 *
 * Note that it is highly recommended to use T-Torch high level API.
 * @see MapMatching
 */
public class TorGraph {


    private static Map<String, TorGraph> instances = new HashMap<>();
    private static final double SPARSE_THRESHOLD = 50;

    public String vehicleType;
    public String OSMPath;


    private GraphHopper hopper;
    private static Logger logger = LoggerFactory.getLogger(TorGraph.class);

    public boolean isBuilt = false;

    public boolean isSaved =false;

    private FileSetting setting;

    final Map<String, TorVertex> allPoints;

    int preComputationRange;
    ShortestPathCache pool;

    // note the values of the map are not unique.
    // if the edge is bidirectional, there will be two entries for the edge. keys are: p1.hash+p2.hash and p2.hash+p1.hash
    // for edges between the same tower points, only the shortest one will be stored
    public final Map<String, TorEdge> allEdges;
    public final Map<String, TowerVertex> towerVertexes;

    RTree<TorVertex, Geometry> rTree;

    //key -- lat, lng hash using GeoHash library
    //value -- id
    public final Map<String, Integer> vertexIdLookup;
    public Map<Integer, TowerVertex> idVertexLookup;
    //key -- concat hash of two tower points
    //value -- id
    public final Map<String, Integer> edgeIdLookup;

    private String baseDir;

    private TorGraph(){
        this.towerVertexes = new HashMap<>();
        this.allPoints = new HashMap<>();
        this.allEdges = new HashMap<>();
        this.rTree = RTree.star().maxChildren(6).create();

        vertexIdLookup = new HashMap<>();
        edgeIdLookup = new HashMap<>();

        pool = new ShortestPathCache();
    }

    public static TorGraph newInstance(String instance_name, FileSetting setting){
        if (instances.containsKey(instance_name)) {
            logger.error("trying to create another graph instance that has the same name with existed one");
            throw new RuntimeException("trying to create another graph instance that has the same name with existed one");
        }

        TorGraph graph = new TorGraph();
        graph.setting = setting;
        instances.put(instance_name, graph);
        graph.baseDir=setting.TorchBase.split("/")[setting.TorchBase.split("/").length-2];
        return graph;
    }

    public static TorGraph getInstance(String instance_name){
        if (!instances.containsKey(instance_name)){
            logger.error("trying to a graph instance that does not exists");
            throw new RuntimeException("trying to a graph instance that does not exists");
        }

        return null;
    }

    /**
     * T-Torch use graph-hopper module to parse and build graph data from .osm file.
     * The method calls graph hopper subroutine.
     *
     * @param OSMPath the path to .osm file
     * @param hopperDataPath the directory for which output goes
     * @param vehicle vehicle type where the trajectory data generated
     * @return this
     */
    public TorGraph initGH(String hopperDataPath, String OSMPath, String vehicle){

        if (hopper != null) {
            logger.error("Torch currently do not support re-initialize graph-hopper in the application lifeCycle");
            return this;
        }
        this.vehicleType=vehicle;
        logger.info("from {}, reading graph data into memory", OSMPath);
        //build hopper
        hopper = new GraphHopperOSM();
        hopper.setDataReaderFile(OSMPath);
        hopper.setGraphHopperLocation(hopperDataPath);
        hopper.setEncodingManager(EncodingManager.create(this.vehicleType));
        hopper.setProfiles(new Profile(this.vehicleType)
                .setVehicle(this.vehicleType));
        hopper.importOrLoad();
        logger.info("have read graph data into memory");

        this.vehicleType = vehicle;
        this.OSMPath = OSMPath;

        return this;
    }

    /**
     * build torGraph.
     */
    public TorGraph build(MMProperties props){

        if (hopper == null)
            throw new IllegalStateException("please invoke " +
                    "'TorGraph initGH(String targetPath, String OSMPath,FlagEncoder vehicle)' first");
        if (isBuilt) {
            logger.warn("trying to build graph twice.");
            return this;
        }

        isBuilt = true;

        buildTorGraph();
        initLookUpTable();

        if (props.mmAlg.equals(Torch.Algorithms.HMM_PRECOMPUTED))
            buildShortestPathCache(props.preComputationRange);

        return this;
    }

    public  TorGraph buildFromDiskData()  {
        if (hopper == null)
            throw new IllegalStateException("please invoke " +
                    "'TorGraph initGH(String targetPath, String OSMPath,FlagEncoder vehicle)' first");
        if (isBuilt) {
            logger.warn("trying to build graph twice.");
            return this;
        }
        Transaction transaction=Transaction.getInstance();
        idVertexLookup = new HashMap<>();
        //read id vertex lookup table
        //todo id_vertex在这里读取
        String id_vertex = getFileNameWithoutExtension(setting.ID_VERTEX_LOOKUP);
        PlainSelect plainSelect = new PlainSelect().withFromItem(new Table(id_vertex));
        plainSelect.addSelectItems(new AllColumns());
        EqualsTo where = new EqualsTo(new Column().withColumnName("traj_name"), new StringValue(this.baseDir));
        plainSelect.setWhere(where);
        WeakReference<SelectResult> id_vertex_result = new WeakReference<>(transaction.query(new Select().withSelectBody(plainSelect)));

        for (Tuple tuple : id_vertex_result.get().getTpl().tuplelist) {
            int id = ((Long) tuple.tuple[1]).intValue();
            double lat = Double.parseDouble((String) tuple.tuple[2]);
            double lng = Double.parseDouble((String) tuple.tuple[3]);
            TowerVertex temp = new TowerVertex(lat, lng, id);
            towerVertexes.put(temp.hash, temp);
            idVertexLookup.put(id, temp);
        }

        //todo id_edge表读的读取在这里改
        //read id edge lookup table
        String id_edge = getFileNameWithoutExtension(setting.ID_EDGE_LOOKUP);
        plainSelect = new PlainSelect().withFromItem(new Table(id_edge));
        plainSelect.addSelectItems(new AllColumns());
        where = new EqualsTo(new Column().withColumnName("traj_name"), new StringValue(this.baseDir));
        plainSelect.setWhere(where);
        WeakReference<SelectResult> id_edge_result = new WeakReference<>(transaction.query(new Select().withSelectBody(plainSelect)));
        for (Tuple tuple:
                id_edge_result.get().getTpl().tuplelist) {
            int edgeId = ((Long) tuple.tuple[1]).intValue();
            int vertexId1 = ((Long) tuple.tuple[2]).intValue();
            int vertexId2 = ((Long) tuple.tuple[3]).intValue();
            double len = Double.parseDouble((String)tuple.tuple[4]);
            TowerVertex t1 = idVertexLookup.get(vertexId1);
            TowerVertex t2 = idVertexLookup.get(vertexId2);
            allEdges.put(t1.hash+ t2.hash, new TorEdge(edgeId, t1, t2, len));
        }
        isBuilt = true;
        System.gc();
        return this;
    }

    /**
     * buildTorGraph a virtual graph from osm data loaded by graph-hopper
     */
    private void buildTorGraph(){

        logger.info("building virtual graph.");
        //model all tower vertexes.
        Graph graph = hopper.getGraphHopperStorage().getBaseGraph();
        NodeAccess nodeAccessor = graph.getNodeAccess();
        logger.info("total number of tower nodes in the graph {}", graph.getNodes());
        for (int i = 0; i < graph.getNodes(); ++i) {
            TowerVertex vertex = new TowerVertex(nodeAccessor.getLat(i), nodeAccessor.getLon(i), i);
            towerVertexes.put(vertex.hash, vertex);
            allPoints.put(vertex.hash, vertex);
        }
        MemoryUsage.printCurrentMemUsage("[after loadding all vertices]");
        AllEdgesIterator allEdgeIterator = graph.getAllEdges();
        // model all edges; two issues are taken into consideration:
        // - since there are a few graph-hopper edges that contains tower points in the middle
        //   (taking beijing.osm as example, there are 54406 tower nodes, while 11 tower nodes in the middle of an edge)
        //   when encounter it, we split it to make sure one edge has exactly two tower points.
        // - there could be more than one edge from one tower point to the other. we will have the shortest one
        logger.info("total number of edges in the graph {}", allEdgeIterator.length());
        while (allEdgeIterator.next()) {

            //fetch tower points as well as pillow points
            PointList pointList = allEdgeIterator.fetchWayGeometry(FetchMode.ALL);
            TowerVertex baseVertex = towerVertexes.get(GeoHash.encodeHash(pointList.getLat(0), pointList.getLon(0)));
            TorEdge edge = new TorEdge();

            int size = pointList.size();
            for (int i = 1; i < size; ++i) {

                PillarVertex pVertex = new PillarVertex(pointList.getLat(i), pointList.getLon(i), edge);

                // the point is a pillar point
                if (!towerVertexes.containsKey(pVertex.hash)) {
                    edge.addPillarVertex(pVertex);
                    allPoints.put(pVertex.hash, pVertex);
                }

                // the point is an tower point
                else {
                    TowerVertex tVertex = towerVertexes.get(pVertex.hash);

                    edge.baseVertex = baseVertex;
                    edge.adjVertex = tVertex;

                    if (allEdgeIterator.getBaseNode()==edge.baseVertex.getId()) {
                        TorEdge oldEdge = allEdges.get(TorEdge.getKey(baseVertex, tVertex));
                        if (oldEdge == null || oldEdge.getLength() > edge.getLength()) {
                            allEdges.put(TorEdge.getKey(baseVertex, tVertex), edge);
                            baseVertex.addAdjPoint(tVertex, edge.getLength());
                        }
                        edge.isForward = true;

                    }

                    if (allEdgeIterator.getBaseNode() == edge.adjVertex.getId()) {
                        TorEdge oldEdge = allEdges.get(TorEdge.getKey(tVertex, baseVertex));
                        if (oldEdge == null || oldEdge.getLength() > edge.getLength()) {
                            allEdges.put(TorEdge.getKey(baseVertex, tVertex), edge);
                            tVertex.addAdjPoint(baseVertex, edge.getLength());
                        }
                        edge.isForward = true;
                    }

                    if (i == size - 1) break;

                    // it happens when an edge has tower points in the middle.
                    // we separate the edge into more.
                    edge = new TorEdge();
                    baseVertex = tVertex;
                }
            }
        }

        MemoryUsage.printCurrentMemUsage("[after loading all edges]");

        // indexAll density to sparse edge to increase map-matching accuracy.
        for (TorEdge edge : allEdges.values()) {
            if (edge.getPillarVertexes().size() == 0 && GeoUtil.distance(edge.baseVertex, edge.adjVertex) >= SPARSE_THRESHOLD * 2) {

                PillarVertex pVertex = PillarVertex.generateMiddle(edge.baseVertex, edge.adjVertex, edge);
                edge.addPillarVertex(pVertex);
                allPoints.put(pVertex.hash, pVertex);
            }

            if (edge.isForward) {
                TorVertex pre = edge.baseVertex;
                List<TorVertex> pillarPoints = (List<TorVertex>)(Object)edge.getPillarVertexes();
                pillarPoints.add(edge.adjVertex);
                for (int i = 0; i < pillarPoints.size(); ++i) {
                    double dis = GeoUtil.distance(pre, pillarPoints.get(i));
                    if (dis >= SPARSE_THRESHOLD * 1.5) {

                        PillarVertex pillarPoint = PillarVertex.generateMiddle(pre, pillarPoints.get(i), edge);
                        pillarPoints.add(i, pillarPoint);
                        allPoints.put(pillarPoint.hash, pillarPoint);
                        --i;
                    } else pre = pillarPoints.get(i);
                }
                pillarPoints.remove(edge.adjVertex);
            }

            if (edge.isBackward) {
                TorVertex pre = edge.adjVertex;
                List<TorVertex> pillarPoints = (List<TorVertex>)(Object)edge.getPillarVertexes();
                pillarPoints.add(0, edge.baseVertex);
                for (int i = pillarPoints.size() - 1; i >= 0; --i) {
                    double dis = GeoUtil.distance(pre, pillarPoints.get(i));
                    if (dis >= SPARSE_THRESHOLD * 1.5) {
                        PillarVertex pillarPoint = PillarVertex.generateMiddle(pre, pillarPoints.get(i), edge);
                        pillarPoints.add(i, pillarPoint);
                        allPoints.put(pillarPoint.hash, pillarPoint);
                        i += 2;
                    } else
                        pre = pillarPoints.get(i);
                }
                pillarPoints.remove(edge.baseVertex);
            }
        }

        //index all the vertexes into Rtree
        for (TorVertex vertex : allPoints.values()) {
            rTree = rTree.add(vertex, Geometries.pointGeographic(vertex.getLng(), vertex.getLat()));
        }

        MemoryUsage.printCurrentMemUsage("[after inserting all vertices to rtree]");
        logger.info("Exit - graph has been built. There are {} edges, {} points including {} tower points.", allEdges.size(), allPoints.size(), towerVertexes.size());
    }

    private TorGraph initLookUpTable(){
        Map<String, TorEdge> edges = allEdges;
        Collection<TowerVertex> vertices = towerVertexes.values();

        for (TowerVertex vertex : vertices)
            vertexIdLookup.put(vertex.hash, vertex.id);

        for (Map.Entry<String, TorEdge> entry : edges.entrySet())
            edgeIdLookup.put(entry.getKey(), entry.getValue().id);

        return this;
    }

    public GraphHopper getGH() {
        return hopper;
    }

    /**
     * Instantiate ShortestPathCache for precomputed map-matching algorithm.
     */
    private void buildShortestPathCache(int precomputationRange) {

        this.preComputationRange = precomputationRange;

        if (!pool.isEmpty())
            return;

        logger.info("Enter - buildShortestPathCache");
        logger.info("precompute shortest path using TorDijkstra (SPFA)");

        pool.init(this);
    }
}