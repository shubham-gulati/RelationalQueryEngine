package dubstep.Optimizer;

import dubstep.CustomClasses.CustomPair;
import dubstep.Main;
import dubstep.Row.Row;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**Author : Shubham Gulati **/

public class NormalComparator implements Comparator<Row> {

    List<Column> toSortOn;
    Map<String, CustomPair<Integer, String>> tempMap;

    public NormalComparator(List<Column> list, Map<String, CustomPair<Integer, String>> mp1) {
        this.toSortOn = list;
        this.tempMap = mp1;

//        for (int i=0;i<this.toSortOn.size();i++) {
//            System.out.println("sort on "+toSortOn.get(i).toString());
//        }
//
//        System.out.println("temp map "+tempMap.get(this.toSortOn.get(0).toString()).getFirst());
        System.out.println(this.toSortOn.get(0).toString());
    }

    public int searchIndex(String col) {
        return tempMap.get(col).getFirst();
    }


    @Override
    public int compare(Row o1, Row o2) {

        for (int i = 0; i< this.toSortOn.size(); i++) {


            int index = searchIndex(this.toSortOn.get(i).toString());
            PrimitiveValue val1 = o1.RowValues.get(index);
            //System.out.println("val 1: "+val1.toString());
            PrimitiveValue val2 = o2.RowValues.get(index);
            //System.out.println("val 2: "+val2.toString());

            if (val1 instanceof LongValue) {
                try {
                    return (Long.compare(val1.toLong(), val2.toLong()));
                } catch (PrimitiveValue.InvalidPrimitive e) {
                    e.printStackTrace();
                }
            } else if (val1 instanceof DoubleValue) {
                try {
                    return (Double.valueOf(val1.toDouble()).compareTo(Double.valueOf(val2.toDouble())));
                } catch (PrimitiveValue.InvalidPrimitive e) {
                    e.printStackTrace();
                }
            } else if (val1 instanceof StringValue) {
                return val1.toString().compareTo(val2.toString());
            } else if (val1 instanceof DateValue) {
                return ((DateValue)val1).getValue().compareTo(((DateValue)val2).getValue());
            }
        }
//        System.out.println("returning zero normal comp");
        return 0;
    }
}
