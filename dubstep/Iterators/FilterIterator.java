package dubstep.Iterators;

import dubstep.CustomClasses.CustomPair;
import dubstep.Eval.EvalCustom;
import dubstep.Main;
import dubstep.Row.Row;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class FilterIterator extends Iterator {

    Expression expression;
    EvalCustom evalCustom;
    boolean deleteFlag;

    @Override
    public boolean hasNext() {
        if (this.leftChild.hasNext()) return true;
        else return false;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    @Override
    public Row next() throws SQLException, IOException {

            Row r;
            r = this.leftChild.next();

            if(this.deleteFlag&&!Main.csvTuple){
                return r;
            }
            else {
                if (r == null) {
                    return null;
                }
                if (this.evalCustom.evaluateWhere(r, this.expression) == false)
                    return null;
                else return r;
            }
        }

    @Override
    public void reset() {

    }

    public FilterIterator(Expression expression, Iterator iterator) {
        this.expression = expression;
        this.leftChild = iterator;
        this.iteratorLeftList = new ArrayList<>();
        this.iteratorRightList = new ArrayList<>();
        if (this.leftChild.iteratorLeftList != null) {
            this.iteratorLeftList.addAll(this.leftChild.iteratorLeftList);
        }

        if (this.leftChild.iteratorRightList != null) {
            this.iteratorLeftList.addAll(this.leftChild.iteratorRightList);
        }

        if ((this.leftChild.mp != null)) {
            this.mp = new HashMap<>();
            this.mp.putAll(this.leftChild.mp);
        }

        this.evalCustom = new EvalCustom(this.mp);

        if(this.expression instanceof InverseExpression){
            this.deleteFlag=true;
        }
        else{
            this.deleteFlag=false;
        }
    }

    public String toString(){
        return this.expression.toString();
    }

    public Expression getExpression() {
        return this.expression;
    }
}
