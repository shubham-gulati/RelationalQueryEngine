package dubstep.Eval;

import dubstep.CustomClasses.CustomPair;
import dubstep.Main;
import dubstep.Row.Row;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class EvalCustom extends Eval {

    Row row = null;
    Map<String,CustomPair<Integer,String>> mapping;

    public Boolean evaluateWhere(Row row, Expression e) throws SQLException {

        this.row = row;

        if (eval(e).toBool())
            return true;
        else
            return false;
    }

    public PrimitiveValue evaluateExpression(Row r, Expression e) throws SQLException {
        this.row = r;
        return (eval(e));
    }

    @Override
    public PrimitiveValue eval(Column column)  {

        String colName = column.getColumnName();
        String tableName = column.getTable().getName();
        String WholeName;

        if (tableName == null) {
            WholeName = Main.CtoT.get(colName) + "." + colName;
        } else
            WholeName = tableName + "." + colName;

        int index = mapping.get(WholeName).getFirst();

        return this.row.RowValues.get(index);
    }

    public EvalCustom(HashMap<String, CustomPair<Integer,String>> mapping){
        this.mapping = mapping;
    }
}
