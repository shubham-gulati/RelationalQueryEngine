package dubstep.Iterators;

import dubstep.CustomClasses.CustomPair;
import dubstep.Eval.EvalCustom;
import dubstep.Main;
import dubstep.Row.Row;
import net.sf.jsqlparser.expression.CaseExpression;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

public class UpdateIterator extends Iterator {

    String tableName;
    EvalCustom evalCustom;

    @Override
    public boolean hasNext() {
        return this.leftChild.hasNext();
    }

    @Override
    public Row next() throws SQLException, IOException {
        Row row = this.leftChild.next();
        ArrayList<CustomPair<String,CaseExpression>> caseList=Main.updateCaseMap.get(this.tableName);

        if(row!=null) {
            for (CustomPair customPair : caseList) {
                String colName = (String) customPair.getFirst();
                CaseExpression caseExpression = (CaseExpression) customPair.getSecond();
                int index = this.mp.get(colName).getFirst();
                row.RowValues.set(index, evalCustom.evaluateExpression(row, caseExpression));
            }
        }

        return row;
    }

    @Override
    public void reset() throws IOException {

    }

    public UpdateIterator(String tableName){
        this.tableName=tableName;

        //left child to be set
        //this.leftChild=
        this.mp=this.leftChild.mp;
        this.evalCustom = new EvalCustom(this.mp);
    }
}
