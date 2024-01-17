package au.edu.rmit.bdm.Torch.mapMatching.algorithm;

import au.edu.rmit.bdm.Torch.base.model.*;
import au.edu.rmit.bdm.Torch.base.model.TorEdge;
import au.edu.rmit.bdm.Torch.mapMatching.model.TowerVertex;
import com.github.davidmoten.geo.GeoHash;
import com.graphhopper.matching.EdgeMatch;
import com.graphhopper.matching.MapMatching;
import com.graphhopper.matching.MatchResult;
import com.graphhopper.matching.Observation;
import com.graphhopper.routing.AlgorithmOptions;
import com.graphhopper.storage.Graph;
import com.graphhopper.storage.NodeAccess;
import com.graphhopper.util.EdgeIteratorState;
import com.graphhopper.util.PMap;
import com.graphhopper.util.shapes.GHPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * HiddenMarkovModel is a map-matching algorithm.
 *
 * It is a wrapper class of Graph-hopper HMM implementation.
 * This implementation is optimized for single trajectory map-matching, and
 * it is highly effective. In terms of matching a large set of trajectories,
 * you may use a different implementation version supported by T-Torch.
 *
 * @see MapMatching More details on Hidden Markov Model
 * @see PrecomputedHiddenMarkovModel
 * @see Mappers the place to instantiate HiddenMarkovModel algorithm
 */
public class HiddenMarkovModel implements Mapper {

    private static Logger logger = LoggerFactory.getLogger(HiddenMarkovModel.class);
    private MapMatching hmm;
    private TorGraph torGraph;

    HiddenMarkovModel(TorGraph torGraph, PMap options){
        hmm = new MapMatching(torGraph.getGH(),options);
        this.torGraph = torGraph;
    }

    @Override
    public Trajectory<TowerVertex> match(Trajectory<? extends TrajEntry> in) {
        logger.info("begin projecting query points onto graph");
        logger.info("origin trajectory: {}", in);

        Trajectory<TowerVertex> mappedTrajectory = new Trajectory<>();
        Graph hopperGraph = torGraph.getGH().getGraphHopperStorage().getBaseGraph();
        Map<String, TowerVertex> towerVertexes =  torGraph.towerVertexes;
        Map<String,TorEdge> edges= torGraph.allEdges;

        mappedTrajectory.hasTime = in.hasTime;
        mappedTrajectory.id = in.id;

        List<Observation> queryTrajectory = new ArrayList<>(in.size());
        for (TrajEntry entry: in)
            queryTrajectory.add(new Observation(new GHPoint(entry.getLat(), entry.getLng())));
        MatchResult ret=null;
        ret = hmm.match(queryTrajectory);
        List<EdgeMatch> matches = ret.getEdgeMatches();

        NodeAccess accessor = hopperGraph.getNodeAccess();

        boolean first = true;
        int pre;
        TowerVertex preVertex = null;
        TowerVertex adjVertex;
        int preAdjId = -1;

        for (EdgeMatch match : matches){
            EdgeIteratorState edge = match.getEdgeState();

            pre = edge.getBaseNode();
            int cur = edge.getAdjNode();
            adjVertex = towerVertexes.get(GeoHash.encodeHash(accessor.getLat(cur), accessor.getLon(cur)));

            if (first){
                preVertex = towerVertexes.get(GeoHash.encodeHash(accessor.getLat(pre), accessor.getLon(pre)));
                mappedTrajectory.add(preVertex);
                first = false;
            }else{
//                try {
//                    assert (preAdjId == pre);
//                } catch (AssertionError e) {
//                    System.err.println("Assertion Error: " + e.getMessage());
//                    // 其他异常处理逻辑
//                }
//                assert (preAdjId == pre);
            }

            mappedTrajectory.add(adjVertex);
        }

        for ( int i = 1; i < mappedTrajectory.size(); i++){
            TorEdge edge = edges.get(TorEdge.getKey(mappedTrajectory.get(i-1), mappedTrajectory.get(i)));
            if (edge == null)
                edge = edges.get(TorEdge.getKey(mappedTrajectory.get(i), mappedTrajectory.get(i-1)));
            if (edge == null){
                System.err.println(mappedTrajectory.get(i-1).id);
                System.err.println(mappedTrajectory.get(i).id);
                System.exit(1);
            }
            edge.setPosition(i);
            mappedTrajectory.edges.add(edge);
        }

//        logger.info("have done map-matching for query.txt points");
//        logger.info("map-matched query vertices representation: {}", mappedTrajectory);
//        logger.info("map-matched edge edges representation: {}", mappedTrajectory.edges);
        return mappedTrajectory;
    }

    @Override
    public <T extends TrajEntry>List<Trajectory<TowerVertex>> batchMatch(List<Trajectory<T>> in) {

        List<Trajectory<TowerVertex>> mappedTrajectories = new ArrayList<>(in.size());
//        ExecutorService threadPool = new ThreadPoolExecutor(10, 20, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
        for (Trajectory<T> raw : in){
//            threadPool.execute(() -> {
                try {
                    Trajectory<TowerVertex> mappedTrajectory = match(raw);
                    mappedTrajectories.add(mappedTrajectory);
                } catch (Exception e) {
                    logger.error("failed to map-matching trajectory {}", raw.id);
                    logger.error("exception: {}", e.getMessage());
                }
//
        }
        logger.info("mappedTrajectories size: " + mappedTrajectories.size());
        return mappedTrajectories;
    }
}