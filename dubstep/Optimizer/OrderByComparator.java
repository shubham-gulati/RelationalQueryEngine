package dubstep.Optimizer;

import dubstep.Main;
import dubstep.Row.Row;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.*;

/**Author : Shubham Gulati **/

public class OrderByComparator implements Comparator<Row> {

    String tableName;
    List<Integer> arrayList;

    public OrderByComparator() {
        this.arrayList = new ArrayList<>();
        setIndexes();
    }

    public void setIndexes() {
        for (int i=0; i<Main.orderByElements.size();i++) {
            //System.out.println(Main.orderByElements.get(i).getExpression().toString());
            int index = search(Main.orderByElements.get(i).getExpression().toString());
            //System.out.println("index is "+index);
            this.arrayList.add(index);
        }
    }

    public int search(String col) {
        List<SelectItem> selectItems = Main.items;
        String colName = "";
        for (int i=0; i<selectItems.size(); i++) {

            if (selectItems.get(i) instanceof SelectExpressionItem) {
                if (((SelectExpressionItem) selectItems.get(i)).getExpression() instanceof BinaryExpression) {
                    SelectExpressionItem selectExpressionItem = ((SelectExpressionItem) selectItems.get(i));
                    if (selectExpressionItem.getAlias() != null) {
                        System.out.println(selectExpressionItem.getAlias());
                        colName = selectExpressionItem.getAlias();
                    } else {
                        Expression expression = ((BinaryExpression) selectExpressionItem.getExpression()).getLeftExpression();
                        while (expression instanceof BinaryExpression) {
                            expression = ((BinaryExpression) expression).getLeftExpression();
                        }
                        colName = expression.toString();
                    }
                } else if (((SelectExpressionItem) selectItems.get(i)).getExpression() instanceof Function) {

                    SelectExpressionItem selectExpressionItem = ((SelectExpressionItem) selectItems.get(i));
                    if (selectExpressionItem.getAlias() != null) {
                        colName = selectExpressionItem.getAlias();
                    } else {
                        colName = ((Function) selectExpressionItem.getExpression()).getParameters().getExpressions().get(0).toString();
                    }
                } else {
                    colName = selectItems.get(i).toString();
                }
            }
//            System.out.println("colname is "+colName);
//            System.out.println("col is "+col);

            if (colName.equals(col) || colName.contains(col)) {
                return i;
            } else {
                continue;
            }
        }

//        System.out.println("colname is "+colName);
//        System.out.println("col is "+col);
//        System.out.println("returning -1");
        return -1;
    }

    @Override
    public int compare(Row o1, Row o2) {
        for (int i=0;i<Main.orderByElements.size();i++) {

            int return_value = 0;
            boolean isAsc = Main.orderByElements.get(i).isAsc();

            this.tableName = Main.CtoT.get(Main.orderByElements.get(i).getExpression().toString());
            int index = this.arrayList.get(i);


            PrimitiveValue val1 = o1.RowValues.get(index);
            PrimitiveValue val2 = o2.RowValues.get(index);


            if (val1 instanceof LongValue) {
                try {
                    return_value = (Long.compare(val1.toLong(), val2.toLong()));
                } catch (PrimitiveValue.InvalidPrimitive e) {
                    e.printStackTrace();
                }
            } else if (val1 instanceof DoubleValue) {

                try {

                    return_value  =  (Double.valueOf(val1.toDouble()).compareTo(Double.valueOf(val2.toDouble())));
                } catch (PrimitiveValue.InvalidPrimitive e) {
                    e.printStackTrace();
                }
            } else if (val1 instanceof StringValue) {

                return_value = val1.toString().compareTo(val2.toString());
            } else if (val1 instanceof DateValue) {
                return_value = ((DateValue)val1).getValue().compareTo(((DateValue)val2).getValue());
            }

            if (return_value != 0) {
                if (isAsc) {
                    return return_value;
                } else {
                    return -1*return_value;
                }
            } else {
                continue;
            }
        }
        return 0;
    }
}
