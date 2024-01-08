package edu.whu.tmdb.query.utils;/*
 * className:Helper
 * Package:edu.whu.tmdb.query.utils
 * Description:
 * @Author: xyl
 * @Create:2024/1/4 - 14:41
 * @Version:v1
 */

import au.edu.rmit.bdm.Torch.mapMatching.model.TowerVertex;
import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.storage.memory.Tuple;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.lang.ref.WeakReference;

import static edu.whu.tmdb.util.FileOperation.getFileNameWithoutExtension;

public class Helper {
    public static void to(){

        String id_vertex = "id_vertex";
        PlainSelect plainSelect = new PlainSelect().withFromItem(new Table(id_vertex));
        plainSelect.addSelectItems(new AllColumns());
        EqualsTo where = new EqualsTo(new Column().withColumnName("traj_name"), new StringValue("Torch"));
        plainSelect.setWhere(where);
        WeakReference<SelectResult> id_vertex_result = new WeakReference<>(Transaction.getInstance().query(new Select().withSelectBody(plainSelect)));

    }
}
