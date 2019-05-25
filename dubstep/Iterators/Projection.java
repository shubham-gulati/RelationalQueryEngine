package dubstep.Iterators;

import dubstep.CustomClasses.CustomPair;
import dubstep.Eval.EvalCustom;
import dubstep.Main;
import dubstep.Row.Row;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class Projection extends Iterator {

    public List<SelectItem> list;
    public boolean aggregate;
    public String Alias2;

    @Override
    public boolean hasNext() {
        if (this.leftChild.hasNext()) return true;
        else return false;
    }

    @Override
    public Row next() throws SQLException, IOException {

        List<PrimitiveValue> rowIndex=new ArrayList<>();

        EvalCustom evalCustom = new EvalCustom(this.mp);

        //if (this.leftChild.hasNext()) {
            if (list.get(0) instanceof AllColumns || aggregate) {
                return this.leftChild.next();
            } else {
                Row row = this.leftChild.next();
                if (row != null) {
                    for (SelectItem selectItem : list) {
                        if (selectItem instanceof AllTableColumns) {
                            AllTableColumns allTableColumns = (AllTableColumns) selectItem;
                            String tableName = allTableColumns.getTable().getName();
                            String ActualTableName = Main.aliases.get(tableName);
                            for (int i = 0; i < Main.coltoDt.get(ActualTableName).size(); i++) {
                                String WholeName = tableName + "." + Main.coltoDt.get(ActualTableName).get(i).getFirst();
                                int index = this.mp.get(WholeName).getFirst();
                                rowIndex.add(row.RowValues.get(index));
                                //if (Alias2 != null)
                                    //RowMap.put(Main.coltoDt.get(ActualTableName).get(i).getFirst() + "." + Main.coltoDt.get(ActualTableName).get(i).getSecond(), row.RowValues.get(WholeName));
                                //else
                                    //RowMap.put(WholeName, row.RowValues.get(WholeName));
                            }
                        } else {
                            if (((SelectExpressionItem) selectItem).getExpression() instanceof BinaryExpression) {
                                BinaryExpression expression = (BinaryExpression) ((SelectExpressionItem) selectItem).getExpression();
                                PrimitiveValue primitiveValue = evalCustom.evaluateExpression(row, expression);
//                                if(Alias2!=null) {
//                                    String DataType = null;
//                                    Column c = (Column) expression.getLeftExpression();
//                                    String tableName;
//                                    String colName = c.getColumnName();
//                                    if (c.getTable().getName() != null)
//                                        tableName = c.getTable().getName();
//                                    else
//                                        tableName = Main.CtoT.get(colName);
//                                    ArrayList<CustomPair<String, String>> temp = Main.coltoDt.get(tableName);
//                                    for (CustomPair<String, String> c1 : temp) {
//                                        if (c1.getFirst().equals(colName)) {
//                                            DataType = c1.getSecond();
//                                            break;
//                                        }
//                                    }
//                                    //expression.getLeftExpression().toString()
//                                    RowMap.put((((SelectExpressionItem) selectItem).getAlias() + "." + DataType), primitiveValue);
//                                }
//                                else
                                    rowIndex.add(primitiveValue);
                                    //RowMap.put((((SelectExpressionItem) selectItem).getAlias()+"."+"afgh"), primitiveValue);

                                //RowMap.put(((SelectExpressionItem) selectItem).getAlias() + "." + Main.coltoDt.get(expression.getLeftExpression().toString()), primitiveValue);
                            } else {
                                Column column = (Column) ((SelectExpressionItem) selectItem).getExpression();
                                String alias=((SelectExpressionItem) selectItem).getAlias();
                                String colName = column.getColumnName();
                                String tableName = column.getTable().getName();
                                String WholeName;

                                if (tableName != null) {
                                    WholeName = tableName + "." + colName;
                                    int index=this.mp.get(WholeName).getFirst();
                                    rowIndex.add(row.RowValues.get(index));
//                                    if (Alias2 != null) {
////                                        for(String key:row.RowValues.keySet()){
////                                            System.out.println("ColNames "+key);
////                                        }
//                                        String DataType = null;
//                                        ArrayList<CustomPair<String, String>> temp = Main.coltoDt.get(tableName);
//                                        //System.out.println(temp.size());
//                                        for (CustomPair<String, String> c : temp) {
//                                            if (c.getFirst().equals(colName)) {
//                                                DataType = c.getSecond();
//                                                break;
//                                            }
//                                        }
//                                        if(alias==null)
//                                            RowMap.put(colName + "." + DataType, row.RowValues.get(column.toString()));
//                                        else
//                                            RowMap.put(alias + "." + DataType, row.RowValues.get(column.toString()));
//                                    } else
//                                        RowMap.put(WholeName, row.RowValues.get(column.toString()));
                                } else {
                                    WholeName = Main.CtoT.get(colName) + "." + colName;
                                    int index=this.mp.get(WholeName).getFirst();
                                    rowIndex.add(row.RowValues.get(index));
//                                    if (Alias2 != null) {
//
//                                        String DataType = null;
//                                        ArrayList<CustomPair<String, String>> temp = Main.coltoDt.get(Main.CtoT.get(colName));
//                                        for (CustomPair<String, String> c : temp) {
//                                            if (c.getFirst().equals(colName)) {
//                                                DataType = c.getSecond();
//                                                break;
//                                            }
//                                        }
//
//                                        if(alias==null)
//                                            RowMap.put(colName + "." + DataType, row.RowValues.get(WholeName));
//                                        else
//                                            RowMap.put(alias + "." + DataType, row.RowValues.get(WholeName));
//
//                                    } else
//                                        RowMap.put(WholeName, row.RowValues.get(WholeName));
                                }
                            }
                        }
                    }
                    Row r = new Row();
                    r.RowValues = rowIndex;
                    return r;
                } else
                    return null;
            }
        }
//        return null;
//    }

    @Override
    public void reset() {

    }

    public Projection(List<SelectItem> selectItem, boolean aggregate, String Alias2) {
        this.list = selectItem;
        this.aggregate = aggregate;
        this.Alias2 = Alias2;
    }

    public void setMap() {
        if ((this.leftChild.mp != null)) {
            this.mp = new HashMap<>();
            this.mp.putAll(this.leftChild.mp);
        }
    }
}
