package au.edu.rmit.bdm.Torch.base.db;

import au.edu.rmit.bdm.Torch.base.FileSetting;

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

import java.util.HashMap;
import java.util.Map;

import static edu.whu.tmdb.util.FileOperation.getFileNameWithoutExtension;

public abstract class TrajectoryPool {

    private Logger logger = LoggerFactory.getLogger(TrajVertexRepresentationPool.class);
    private boolean isMem;
    private Map<String, String[]> memPool;

    String tableName;

    FileSetting setting;

    TrajectoryPool(boolean isMem, FileSetting setting)  {
        this.setting=setting;

        this.isMem = isMem;
//        if (!isMem) {
//            logger.info("init Torch_Porto.db version trajectory representation pool");
////            db = DBManager.getDB();
//            return;
//        }

        logger.info("init memory version trajectory representation pool");
        memPool = new HashMap<>();

        loadFromFile((this instanceof TrajVertexRepresentationPool) ?
                setting.TRAJECTORY_VERTEX_REPRESENTATION_PATH_PARTIAL :
                setting.TRAJECTORY_EDGE_REPRESENTATION_PATH_PARTIAL);
        //read meta properties
    }

    //todo 这里改两个partial的信息
    private void loadFromFile(String filePath)  {
        String table = getFileNameWithoutExtension(filePath);
        PlainSelect plainSelect = new PlainSelect().withFromItem(new Table(table));
        plainSelect.addSelectItems(new AllColumns());
        EqualsTo where = new EqualsTo(new Column().withColumnName("traj_name"), new StringValue(setting.TorchBase));
        plainSelect.setWhere(where);
        SelectResult result = Transaction.getInstance().query(new Select().withSelectBody(plainSelect));
        for (Tuple tuple :
                result.getTpl().tuplelist) {
            memPool.put((String)tuple.tuple[0]
                    , ((String)tuple.tuple[1]).split(","));
        }

    }

    public int[] get(String trajId)  {

        int[] ret;

        if (isMem){
            String[] trajectory = memPool.get(trajId);
            ret = new int[trajectory.length];
            for (int i = 0; i < ret.length; i++)
                ret[i] = Integer.valueOf(trajectory[i]);
        }else{
            String sql="select edges from "+getFileNameWithoutExtension(setting.TRAJECTORY_EDGE_REPRESENTATION_PATH_PARTIAL)
                    +" where id="+trajId
                    +" and traj_name='"+getFileNameWithoutExtension(setting.TorchBase)+"';";
            SelectResult query = Transaction.getInstance().query(sql);
            String[] temp = new String[0];
            if(!query.getTpl().tuplelist.isEmpty()){
                Tuple tuple = query.getTpl().tuplelist.get(0);
                temp = ((String)tuple.tuple[0]).split(",");
            }
//            String[] temp = db.get(tableName, trajId).split(",");
            ret = new int[temp.length];
            for (int i = 0; i < temp.length; i++)
                ret[i] = Integer.valueOf(temp[i]);
        }
        return ret;
    }

    public Map<String, String[]> getMemPool() {
        return memPool;
    }

    public void setMemPool(Map<String, String[]> memPool) {
        this.memPool = memPool;
    }
}
