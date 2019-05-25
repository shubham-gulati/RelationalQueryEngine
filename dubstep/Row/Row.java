package dubstep.Row;

import net.sf.jsqlparser.expression.PrimitiveValue;

import java.io.Serializable;
import java.util.*;

public class Row implements Serializable {

    public List<PrimitiveValue> RowValues;
    public int indexRow;

    public Row(){
        RowValues= new ArrayList<>();
        indexRow = -1;
    }

    public List<PrimitiveValue> getRowValues() {
        return RowValues;
    }

    @Override
    public String toString() {
        String val = "";
        List<PrimitiveValue> l = getRowValues();
        if (getRowValues().size() > 0) {
            val+=getRowValues().get(0);
            for (int i = 1; i < l.size(); i++) {
                val +="|"+getRowValues().get(i);
            }
        }
        return val;
    }
}
