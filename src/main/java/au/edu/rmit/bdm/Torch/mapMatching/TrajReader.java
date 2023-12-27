package au.edu.rmit.bdm.Torch.mapMatching;

import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.base.model.TrajNode;
import au.edu.rmit.bdm.Torch.base.model.Trajectory;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.utils.SelectResult;
import edu.whu.tmdb.query.utils.traj.TrajTrans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.List;

import static edu.whu.tmdb.util.FileOperation.getFileNameWithoutExtension;

/**
 * The class is for reading trajectories following the certain format from file.
 */
public class TrajReader {

    private final int BATCH_SIZE;
    private static Logger logger = LoggerFactory.getLogger(TrajReader.class);
    private static LineNumberReader trajReader;
    private static BufferedReader dateReader = null;

    public TrajReader(){
        BATCH_SIZE = 100000;
    }

    public TrajReader(MMProperties props){
        BATCH_SIZE = props.batchSize;
    }

    /**
     * Read raw trajectories.
     * Trajectories that do not follow the format or contain illegal data will be discarded.
     *
     * @param trajSrcPath File containing trajectories.
     * @param dateDataPath File containing timestamp of nodes in trajectories.
     *                 This file could be null and if this is the case, the program will leave time field in trajectory model blank.
     * @return a list of trajectories.
     */
    public boolean readBatch(String trajSrcPath, File dateDataPath, List<Trajectory<TrajEntry>> trajectoryList) {

        logger.info("now reading trajectories");

        trajectoryList.clear();
        boolean hasDate = (dateDataPath != null);
        boolean finished = false;
        SimpleDateFormat sdfmt = null;
        if (hasDate) sdfmt = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Transaction transaction = Transaction.getInstance();
        SelectResult query=null;
        String sql="select * from traj" +
                " where traj_name='" + getFileNameWithoutExtension(trajSrcPath) + "';";
        query = transaction.query(sql);
        for (int i = 0; i < query.getTpl().tuplelist.size(); i++) {
            Tuple tuple = query.getTpl().tuplelist.get(i);
            if (i+1 % BATCH_SIZE == 0) {
                logger.info("have readBatch {} trajectories in total", i);
                return false;
            }
            Trajectory<TrajEntry> trajectory = new Trajectory<>(tuple.tuple[0].toString(), false);
            List<TrajEntry> traj = TrajTrans.getTraj((double[])tuple.tuple[3]);
            for (int j = 0; j < traj.size(); j++) {
                trajectory.add(new TrajNode(traj.get(j).getLat(),traj.get(j).getLng()));
            }
            trajectoryList.add(trajectory);
        }
        return true;
    }
}
