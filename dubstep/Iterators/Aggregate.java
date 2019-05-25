package dubstep.Iterators;

import dubstep.Eval.EvalCustom;
import dubstep.Main;
import dubstep.Row.Row;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class Aggregate {

    Projection projection;
    EvalCustom evalCustom;
    PlainSelect plainSelect;
    List<Row> rowList;
    List<SelectItem> list;

    public List<Row> GroupBy() throws IOException, SQLException {
        //final long startTime1 = System.currentTimeMillis();
        Map<String, Integer> GroupHashIndex= new HashMap<>();
        Map<String, Integer> AggregateHashIndex= new HashMap<>();
        Map<Integer, String> AggregateHashIndexInverse= new HashMap<>();
        Map<String, PrimitiveValue[]> FinalIndex= new HashMap<>();
        List<Column> groupbyColumns=plainSelect.getGroupByColumnReferences();
        List<Function> lstFunction = new ArrayList<>();
        rowList = new ArrayList<>();

        int i = 0, k = 0;
        for (Column c : groupbyColumns) {
            GroupHashIndex.put(c.getWholeColumnName(), i);
            i++;
        }

        for (SelectItem s : list) {
            Expression expression = ((SelectExpressionItem) s).getExpression();
            if (expression instanceof Function) {
                lstFunction.add((Function) expression);
                AggregateHashIndex.put(expression.toString(), k);
                AggregateHashIndexInverse.put(k, expression.toString());
                k++;
            }
        }
        AggregateHashIndex.put("Kounter", k);
        AggregateHashIndexInverse.put(k, "Kounter");

        while (projection.hasNext()) {
            PrimitiveValue[] aggGrp = new PrimitiveValue[lstFunction.size() + 1];

            Row d = projection.next();

            if (d != null) {

                PrimitiveValue[] temp = new PrimitiveValue[groupbyColumns.size()];

                for (int j = 0; j < temp.length; j++) {
                    temp[j] = evalCustom.evaluateExpression(d, groupbyColumns.get(j));
                }

                String arrKey = Arrays.toString(temp);

                if (!FinalIndex.containsKey(arrKey)) {
                    for (int j = 0; j < aggGrp.length - 1; j++) {
                        if (lstFunction.get(j).isAllColumns() || lstFunction.get(j).getName().equals("COUNT")) {
                            aggGrp[j] = new LongValue(1);
                        } else {
                            aggGrp[j] = evalCustom.evaluateExpression(d, lstFunction.get(j).getParameters().getExpressions().get(0));
                        }
                    }
                    aggGrp[aggGrp.length - 1] = new LongValue(1);
                    FinalIndex.put(arrKey, aggGrp);
                } else {
                    for (int j = 0; j < FinalIndex.get(arrKey).length - 1; j++) {

                        if (lstFunction.get(j).isAllColumns()) {
                            LongValue l1 = (LongValue) FinalIndex.get(arrKey)[j];
                            Long l2 = l1.toLong() + 1;
                            PrimitiveValue[] temp1 = FinalIndex.get(arrKey);
                            temp1[j] = new LongValue(l2);
                            FinalIndex.put(arrKey, temp1);
                        } else {
                            Expression expression = lstFunction.get(j).getParameters().getExpressions().get(0);
                            String type = AggregateHashIndexInverse.get(j);
                            if (type.startsWith("SUM") || type.startsWith("AVG")) {
                                PrimitiveValue tem = evalCustom.evaluateExpression(d, expression);

                                if (tem.getType().toString().equals("DOUBLE")) {

                                    DoubleValue d1 = (DoubleValue) FinalIndex.get(arrKey)[j];
                                    Double d1Temp = d1.toDouble();
                                    DoubleValue d2 = (DoubleValue) tem;
                                    Double d2Temp = d2.toDouble();
                                    PrimitiveValue[] temp1 = FinalIndex.get(arrKey);
                                    temp1[j] = new DoubleValue(d1Temp + d2Temp);
                                    FinalIndex.put(arrKey, temp1);
                                } else if (tem.getType().toString().equals("LONG")) {
                                    LongValue l1 = (LongValue) FinalIndex.get(arrKey)[j];
                                    Long l1Temp = l1.toLong();
                                    LongValue l2 = (LongValue) tem;
                                    Long l2Temp = l2.toLong();
                                    PrimitiveValue[] temp1 = FinalIndex.get(arrKey);
                                    temp1[j] = new LongValue(l1Temp + l2Temp);
                                    FinalIndex.put(arrKey, temp1);
                                }
                            }
                            if (type.startsWith("MAX")) {
                                PrimitiveValue tem = evalCustom.evaluateExpression(d, expression);
                                if (tem.getType().toString().equals("DOUBLE")) {
                                    DoubleValue d1 = (DoubleValue) FinalIndex.get(arrKey)[j];
                                    Double d1Temp = d1.toDouble();
                                    DoubleValue d2 = (DoubleValue) tem;
                                    Double d2Temp = d2.toDouble();
                                    if (d1Temp < d2Temp) {
                                        PrimitiveValue[] temp1 = FinalIndex.get(arrKey);
                                        temp1[j] = d2;
                                        FinalIndex.put(arrKey, temp1);
                                    }
                                } else if (tem.getType().toString().equals("LONG")) {
                                    LongValue l1 = (LongValue) FinalIndex.get(arrKey)[j];
                                    Long l1Temp = l1.toLong();
                                    LongValue l2 = (LongValue) tem;
                                    Long l2Temp = l2.toLong();
                                    if (l1Temp < l2Temp) {
                                        PrimitiveValue[] temp1 = FinalIndex.get(arrKey);
                                        temp1[j] = l2;
                                        FinalIndex.put(arrKey, temp1);
                                    }
                                }
                            }
                            if (type.startsWith("MIN")) {
                                PrimitiveValue tem = evalCustom.evaluateExpression(d, expression);
                                if (tem.getType().toString().equals("DOUBLE")) {
                                    DoubleValue d1 = (DoubleValue) FinalIndex.get(arrKey)[j];
                                    Double d1Temp = d1.toDouble();
                                    DoubleValue d2 = (DoubleValue) tem;
                                    Double d2Temp = d2.toDouble();
                                    if (d1Temp > d2Temp) {
                                        PrimitiveValue[] temp1 = FinalIndex.get(arrKey);
                                        temp1[j] = d2;
                                        FinalIndex.put(arrKey, temp1);
                                    }
                                } else if (tem.getType().toString().equals("LONG")) {
                                    LongValue l1 = (LongValue) FinalIndex.get(arrKey)[j];
                                    Long l1Temp = l1.toLong();
                                    LongValue l2 = (LongValue) tem;
                                    Long l2Temp = l2.toLong();
                                    if (l1Temp > l2Temp) {
                                        PrimitiveValue[] temp1 = FinalIndex.get(arrKey);
                                        temp1[j] = l2;
                                        FinalIndex.put(arrKey, temp1);
                                    }

                                }
                            }
                            if (type.startsWith("COUNT")) {
                                LongValue l1 = (LongValue) FinalIndex.get(arrKey)[j];
                                Long l2 = l1.toLong() + 1;
                                PrimitiveValue[] temp1 = FinalIndex.get(arrKey);
                                temp1[j] = new LongValue(l2);
                                FinalIndex.put(arrKey, temp1);
                            }
                        }
                    }
                    LongValue l1 = (LongValue) FinalIndex.get(arrKey)[FinalIndex.get(arrKey).length - 1];
                    Long l2 = l1.toLong() + 1;

                    PrimitiveValue[] temp1 = FinalIndex.get(arrKey);
                    temp1[FinalIndex.get(arrKey).length - 1] = new LongValue(l2);
                    FinalIndex.put(arrKey, temp1);

                }
            }
        }
//        final long endTime1 = System.currentTimeMillis();
//        Main.groupByTime=endTime1-startTime1;

        for (String group : FinalIndex.keySet()) {
            String[] sTemp = group.substring(1, group.length() - 1).split(",");
            Row row = new Row();
            for (int j = 0; j < list.size(); j++) {

                String alias = ((SelectExpressionItem) list.get(j)).getAlias();
                Expression expression = ((SelectExpressionItem) list.get(j)).getExpression();

                if (expression instanceof Function) {
                    PrimitiveValue val = FinalIndex.get(group)[AggregateHashIndex.get(expression.toString())];
                    if (((Function) expression).getName().startsWith("AVG")) {
                        if (val.getType().toString().equals("DOUBLE")) {

                            Double d1 = val.toDouble();
                            Double d2 = FinalIndex.get(group)[FinalIndex.get(group).length - 1].toDouble();

                            Double result = d1 / d2;
                            row.RowValues.add(new DoubleValue(result));
//                                        if (alias != null)
//                                            row.RowValues.put(alias, new DoubleValue(result));
//                                        else
//                                            row.RowValues.put(type.toString(), new DoubleValue(result));
                        } else if (val.getType().toString().equals("LONG")) {
                            Long l1 = val.toLong();
                            Long l2 = FinalIndex.get(group)[FinalIndex.get(group).length - 1].toLong();

                            if (l1 % l2 == 0) {
                                Long result = l1 / l2;
                                row.RowValues.add(new LongValue(result));
//                                            if (alias != null)
//                                                row.RowValues.put(alias, new LongValue(result));
//                                            else
//                                                row.RowValues.put(type.toString(), new LongValue(result));
                            } else {
                                Double d1 = Double.parseDouble(String.valueOf(l1));
                                Double d2 = Double.parseDouble(String.valueOf(l2));
                                Double result = d1 / d2;
                                row.RowValues.add(new DoubleValue(result));
//                                            if (alias != null)
//                                                row.RowValues.put(alias, new DoubleValue(result));
//                                            else
//                                                row.RowValues.put(type.toString(), new DoubleValue(result));
                            }

                        }

                    } else {
                        row.RowValues.add(val);

//                                    if (alias != null)
//                                        row.RowValues.put(alias, FinalIndex.get(group)[AggregateHashIndex.get(type.toString())]);
//                                    else
//                                        row.RowValues.put(type.toString(), FinalIndex.get(group)[AggregateHashIndex.get(type.toString())]);
                    }
                } else {
                    String colDatatype = Main.getcolDataType(((SelectExpressionItem) list.get(j)).getExpression().toString());
                    String val = sTemp[GroupHashIndex.get(((SelectExpressionItem) list.get(j)).getExpression().toString())].trim();

                    if (colDatatype.equals("int")) {
                        Long l = Long.parseLong(val);
                        LongValue l1 = new LongValue(l);
                        row.RowValues.add(l1);
                    } else if (colDatatype.equals("string") || colDatatype.equals("varchar") || colDatatype.equals("char")) {
                        StringValue s1 = new StringValue(val);
                        row.RowValues.add(s1);
                    } else if (colDatatype.equals("decimal")) {
                        Double d = Double.parseDouble(val);
                        DoubleValue d1 = new DoubleValue(d);
                        row.RowValues.add(d1);
                    } else if (colDatatype.equals("date")) {
                        DateValue dateValue = new DateValue(val);
                        row.RowValues.add(dateValue);
                    }

                }
            }
            rowList.add(row);

        }

        return rowList;
    }

    public List<Row> AggWithoutGroupBy() throws IOException, SQLException {

        int numRows = 0;
        boolean first = true;
        PrimitiveValue[] agg = new PrimitiveValue[list.size()];
        rowList = new ArrayList<>();

        while (projection.hasNext()) {
            Row d = projection.next();
            if (d != null) {
                numRows++;
                if (first) {

                    for (int j = 0; j < agg.length; j++) {
                        Expression expression = ((SelectExpressionItem) list.get(j)).getExpression();

                        if (expression instanceof Function) {
                            Function f = (Function) expression;
                            if (f.isAllColumns() || f.getName().equals("COUNT")) {
                                agg[j] = new LongValue(1);
                            } else
                                agg[j] = evalCustom.evaluateExpression(d, f.getParameters().getExpressions().get(0));
                        } else {
                            agg[j] = agg[j] = evalCustom.evaluateExpression(d, expression);
                        }

                        first = false;
                    }
                } else {
                    for (int i = 0; i < list.size(); i++) {
                        if (((SelectExpressionItem) list.get(i)).getExpression() instanceof Function) {
                            String name = ((Function) (((SelectExpressionItem) list.get(i)).getExpression())).getName();
                            if (((Function) (((SelectExpressionItem) list.get(i)).getExpression())).isAllColumns()) {
                                LongValue l1 = (LongValue) agg[i];
                                Long l2 = l1.toLong() + 1;
                                agg[i] = new LongValue(l2);
                            } else {
                                Expression type = ((Function) (((SelectExpressionItem) list.get(i)).getExpression())).getParameters().getExpressions().get(0);


                                if (name.equals("SUM") || name.equals("AVG")) {
                                    PrimitiveValue tem = evalCustom.evaluateExpression(d, type);

                                    if (tem.getType().toString().equals("DOUBLE")) {
                                        Double d1Temp = agg[i].toDouble();
                                        Double d2Temp = tem.toDouble();
                                        agg[i] = new DoubleValue(d1Temp + d2Temp);
                                    } else if (tem.getType().toString().equals("LONG")) {
                                        Long l1Temp = agg[i].toLong();
                                        Long l2Temp = tem.toLong();
                                        agg[i] = new LongValue(l1Temp + l2Temp);
                                    }

                                }
                                if (name.equals("MAX")) {
                                    PrimitiveValue tem = evalCustom.evaluateExpression(d, type);
                                    if (tem.getType().toString().equals("DOUBLE")) {
                                        DoubleValue d1 = (DoubleValue) agg[i];
                                        Double d1Temp = agg[i].toDouble();
                                        DoubleValue d2 = (DoubleValue) tem;
                                        Double d2Temp = tem.toDouble();
                                        if (d1Temp < d2Temp) {
                                            agg[i] = d2;
                                        }
                                    } else if (tem.getType().toString().equals("LONG")) {
                                        LongValue l1 = (LongValue) agg[i];
                                        Long l1Temp = l1.toLong();
                                        LongValue l2 = (LongValue) tem;
                                        Long l2Temp = l2.toLong();
                                        if (l1Temp < l2Temp) {
                                            agg[i] = l2;
                                        }

                                    }
                                }
                                if (name.equals("MIN")) {
                                    PrimitiveValue tem = evalCustom.evaluateExpression(d, type);
                                    if (tem.getType().toString().equals("DOUBLE")) {
                                        DoubleValue d1 = (DoubleValue) agg[i];
                                        Double d1Temp = d1.toDouble();
                                        DoubleValue d2 = (DoubleValue) tem;
                                        Double d2Temp = d2.toDouble();
                                        if (d1Temp > d2Temp) {
                                            agg[i] = d2;
                                        }
                                    } else if (tem.getType().toString().equals("LONG")) {
                                        LongValue l1 = (LongValue) agg[i];
                                        Long l1Temp = l1.toLong();
                                        LongValue l2 = (LongValue) tem;
                                        Long l2Temp = l2.toLong();
                                        if (l1Temp > l2Temp) {
                                            agg[i] = l2;
                                        }

                                    }
                                }
                                if (name.equals("COUNT")) {
                                    LongValue l1 = (LongValue) agg[i];
                                    Long l2 = l1.toLong() + 1;
                                    agg[i] = new LongValue(l2);
                                }
                            }
                        } else {
                            agg[i] = evalCustom.evaluateExpression(d, ((SelectExpressionItem) list.get(i)).getExpression());
                        }
                    }
                }
            }
        }
        if (agg[0] != null){
            Row row = new Row();

        for (int i = 0; i < list.size(); i++) {


            if ((((SelectExpressionItem) list.get(i)).getExpression()) instanceof Function) {

                String name = ((Function) (((SelectExpressionItem) list.get(i)).getExpression())).getName();
                String alias = ((SelectExpressionItem) list.get(i)).getAlias();

                if (name.equals("AVG")) {
                    String type = agg[i].getType().toString();
                    if (type.equals("DOUBLE")) {
                        DoubleValue d1 = (DoubleValue) agg[i];
                        Double d1Temp = d1.toDouble();
                        Double d2Temp = Double.valueOf(numRows);
                        Double result = d1Temp / d2Temp;
                        row.RowValues.add(new DoubleValue(result));

                    } else if (type.equals("LONG")) {
                        LongValue l1 = (LongValue) agg[i];
                        Long l1Temp = l1.toLong();
                        Long l2Temp = Long.valueOf(numRows);


                        if (l1Temp % l2Temp == 0) {
                            Long result = l1Temp / l2Temp;
                            row.RowValues.add(new LongValue(result));
                        } else {
                            Double d1 = Double.parseDouble(String.valueOf(l1Temp));
                            Double d2 = Double.parseDouble(String.valueOf(l2Temp));
                            Double result = d1 / d2;
                            row.RowValues.add(new DoubleValue(result));
//                                            if (alias != null)
//                                                row.RowValues.put(alias, new DoubleValue(result));
//                                            else
//                                                row.RowValues.put(type.toString(), new DoubleValue(result));
                        }


                        //row.RowValues.put(alias, new LongValue(result));

                    }
                } else {
                    row.RowValues.add(agg[i]);
                }
            } else {
                row.RowValues.add(agg[i]);
            }


        }
        rowList.add(row);
    }

        return rowList;
    }

    public Aggregate(PlainSelect plainSelect, Projection projection) {

        this.projection=projection;
        this.evalCustom=new EvalCustom(projection.mp);
        this.plainSelect=plainSelect;
        this.list=projection.list;
    }
}
