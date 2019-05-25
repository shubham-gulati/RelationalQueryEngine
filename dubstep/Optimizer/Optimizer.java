package dubstep.Optimizer;

import dubstep.Iterators.*;
import dubstep.Iterators.Iterator;
import dubstep.Main;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**Author : Shubham Gulati **/

public class Optimizer {

    public Optimizer() {

    }
    public List<Expression> whereList = new ArrayList<>();
    public List<Expression> newWhereList = new ArrayList<>();
    public static String ONEPASS = "ONEPASS";
    public static String SORTMERGE = "SORTMERGE";
    public static String LEFT = "LEFT";
    public static String RIGHT = "RIGHT";
    public Iterator finalIterator;
    public Iterator initialFilterIterator;
    Map<String, HashSet<String>> colsMap = new HashMap<>();

    //this will return the linking scan iterator
    public void dfs(Iterator it, String tableName, Iterator parent, Expression expression, String branch) {
        if ((it instanceof ScanIterator) && ((ScanIterator) it).getTableName().equals(tableName)) {

            FilterIterator filterIterator = new FilterIterator(expression, it);
            filterIterator.leftChild = it;
            ArrayList<String> l = new ArrayList<>();
            l.addAll(filterIterator.iteratorLeftList);
            l.addAll(filterIterator.iteratorRightList);

            if (branch.equals(LEFT)) {
                    parent.leftChild = filterIterator;
                    parent.iteratorLeftList = l;
            } else if (branch.equals(RIGHT)) {
                parent.rightChild = filterIterator;
                parent.iteratorRightList = l;
            }

            finalIterator = filterIterator;
            return;


        } else {
            if (it.leftChild != null) {
                dfs(it.leftChild, tableName, it, expression, LEFT);
            }
            if (it.rightChild != null) {
                dfs(it.rightChild, tableName, it, expression, RIGHT);
            }
        }
    }

    public void dfsJoinChanges(Iterator iterator, String tableName1, String tableName2, String joinColName1, String joinColName2,
                                      Iterator parent, Expression expression, String type, String branch) throws IOException, SQLException {


        if ((iterator instanceof NestedLoopJoinIterator) && iterator.iteratorLeftList.contains(tableName1) &&
                iterator.iteratorLeftList.contains(tableName2)) {

            dfsJoinChanges(iterator.leftChild, tableName1, tableName2, joinColName1, joinColName2, iterator, expression, type, LEFT);
        } else if ((iterator instanceof NestedLoopJoinIterator) && iterator.iteratorRightList.contains(tableName1) &&
                iterator.iteratorRightList.contains(tableName2)) {

            dfsJoinChanges(iterator.rightChild, tableName1, tableName2, joinColName1, joinColName2, iterator, expression, type, RIGHT);
        } else {

            if ((iterator instanceof NestedLoopJoinIterator) && iterator.iteratorLeftList.contains(tableName1) &&
                    iterator.iteratorRightList.contains(tableName2) || ((iterator instanceof NestedLoopJoinIterator) && iterator.iteratorLeftList.contains(tableName2) &&
                    iterator.iteratorRightList.contains(tableName1))) {

                if (type.equals(ONEPASS)) {

                    OnePasHashJoin onePasHashJoin = new OnePasHashJoin(iterator.leftChild, iterator.rightChild, expression);
                    ArrayList<String> left = new ArrayList<>();
                    ArrayList<String> right = new ArrayList<>();

                    if (iterator.leftChild.iteratorLeftList != null) {
                        left.addAll(iterator.leftChild.iteratorLeftList);
                    }

                    if (iterator.leftChild.iteratorRightList != null) {
                        left.addAll(iterator.leftChild.iteratorRightList);
                    }

                    if (iterator.rightChild.iteratorLeftList != null) {
                        right.addAll(iterator.rightChild.iteratorLeftList);
                    }

                    if (iterator.rightChild.iteratorRightList != null) {
                        right.addAll(iterator.rightChild.iteratorRightList);
                    }

                    onePasHashJoin.iteratorLeftList = left;
                    onePasHashJoin.iteratorRightList = right;

                    ArrayList<String> parent_new = new ArrayList<>();
                    parent_new.addAll(onePasHashJoin.iteratorLeftList);
                    parent_new.addAll(onePasHashJoin.iteratorRightList);

                    if (branch.equals(LEFT)) {
                        parent.leftChild = onePasHashJoin;
                        parent.iteratorLeftList = parent_new;
                    } else if (branch.equals(RIGHT)) {
                        parent.rightChild = onePasHashJoin;
                        parent.iteratorRightList = parent_new;
                    }

                    finalIterator = onePasHashJoin;
                    return;

                } else if (type.equals(SORTMERGE)) {
                    SortMergeJoin sortMergeJoin = new SortMergeJoin(iterator.leftChild, iterator.rightChild, expression);
                    ArrayList<String> left = new ArrayList<>();
                    ArrayList<String> right = new ArrayList<>();

                    if (iterator.leftChild.iteratorLeftList != null) {
                        left.addAll(iterator.leftChild.iteratorLeftList);
                    }

                    if (iterator.leftChild.iteratorRightList != null) {
                        left.addAll(iterator.leftChild.iteratorRightList);
                    }

                    if (iterator.rightChild.iteratorLeftList != null) {
                        right.addAll(iterator.rightChild.iteratorLeftList);
                    }

                    if (iterator.rightChild.iteratorRightList != null) {
                        right.addAll(iterator.rightChild.iteratorRightList);
                    }

                    sortMergeJoin.iteratorLeftList = left;
                    sortMergeJoin.iteratorRightList = right;

                    ArrayList<String> parent_new = new ArrayList<>();
                    parent_new.addAll(sortMergeJoin.iteratorLeftList);
                    parent_new.addAll(sortMergeJoin.iteratorRightList);
                    if (branch.equals(LEFT)) {
                        parent.leftChild = sortMergeJoin;
                        parent.iteratorLeftList = parent_new;
                    } else if (branch.equals(RIGHT)) {
                        parent.rightChild = sortMergeJoin;
                        parent.iteratorRightList = parent_new;
                    }
                    finalIterator = sortMergeJoin;
                }

            } else if ((iterator instanceof OnePasHashJoin) && iterator.iteratorLeftList.contains(tableName1)
                    && iterator.iteratorRightList.contains(tableName2)) {

                FilterIterator f = new FilterIterator(expression, iterator);
                f.leftChild = iterator;

                ArrayList<String> parent_new = new ArrayList<>();
                if (f.iteratorLeftList != null) {
                    parent_new.addAll(f.iteratorLeftList);
                }

                if (f.iteratorRightList != null) {
                    parent_new.addAll(f.iteratorRightList);
                }

                if (branch.equals(LEFT)) {
                    parent.leftChild = f;
                    parent.iteratorLeftList = parent_new;
                } else if (branch.equals(RIGHT)) {
                    parent.rightChild = f;
                    parent.iteratorRightList = parent_new;
                }

                finalIterator = f;
            }

            else if ((iterator instanceof IndexNestedLoopJoin) && iterator.iteratorLeftList.contains(tableName1)
                        && iterator.iteratorRightList.contains(tableName2)) {

                    FilterIterator f = new FilterIterator(expression, iterator);
                    f.leftChild = iterator;

                    ArrayList<String> parent_new = new ArrayList<>();
                    if (f.iteratorLeftList != null) {
                        parent_new.addAll(f.iteratorLeftList);
                    }

                    if (f.iteratorRightList != null) {
                        parent_new.addAll(f.iteratorRightList);
                    }

                    if (branch.equals(LEFT)) {
                        parent.leftChild = f;
                        parent.iteratorLeftList = parent_new;
                    } else if (branch.equals(RIGHT)) {
                        parent.rightChild = f;
                        parent.iteratorRightList = parent_new;
                    }

                    finalIterator = f;
            }
                else if ((iterator instanceof SortMergeJoin) && iterator.iteratorLeftList.contains(tableName1)
                        && iterator.iteratorRightList.contains(tableName2)) {
                FilterIterator f = new FilterIterator(expression, iterator);
                f.leftChild = iterator;

                ArrayList<String> parent_new = new ArrayList<>();
                if (f.iteratorLeftList != null) {
                    parent_new.addAll(f.iteratorLeftList);
                }

                if (f.iteratorRightList != null) {
                    parent_new.addAll(f.iteratorRightList);
                }

                if (branch.equals(LEFT)) {
                    parent.leftChild = f;
                    parent.iteratorLeftList = parent_new;
                } else if (branch.equals(RIGHT)) {
                    parent.rightChild = f;
                    parent.iteratorRightList = parent_new;
                }

                finalIterator = f;

            } else {
                if (iterator.leftChild != null) {
                    dfsJoinChanges(iterator.leftChild, tableName1, tableName2, joinColName1, joinColName2, iterator, expression, type, LEFT);
                }
                if (iterator.rightChild != null) {
                    dfsJoinChanges(iterator.rightChild, tableName1, tableName2, joinColName1, joinColName2, iterator, expression, type, RIGHT);
                }
            }
        }
    }


    public void AndOrEval(Expression expression){
        if ((expression instanceof AndExpression)) {
            BinaryExpression binaryExpression = (BinaryExpression)expression;
            AndOrEval(binaryExpression.getLeftExpression());
            AndOrEval(binaryExpression.getRightExpression());
        } else {
            whereList.add(expression);
        }
    }

    public void reBuildTree(Iterator iterator) throws IOException, SQLException {

        if (iterator.leftChild instanceof FilterIterator) {
            initialFilterIterator = iterator.leftChild;
            Expression expression = ((FilterIterator) iterator.leftChild).getExpression();

            AndOrEval(expression);

            for (int i=whereList.size()-1; i >= 0; i--) {
                //for every where clause we need to do dfs and modify the tree

                if (whereList.get(i) instanceof EqualsTo) {
                    if ((((EqualsTo) whereList.get(i)).getLeftExpression() instanceof Column) &&
                            (((EqualsTo) whereList.get(i)).getRightExpression() instanceof Column)) {

                        String left_table = ((Column) ((EqualsTo) whereList.get(i)).getLeftExpression()).getTable().getName();
                        String right_table = ((Column) ((EqualsTo) whereList.get(i)).getRightExpression()).getTable().getName();
                        String left_table_colName = ((Column) ((EqualsTo) whereList.get(i)).getLeftExpression()).getColumnName();
                        String right_table_colName = ((Column) ((EqualsTo) whereList.get(i)).getRightExpression()).getColumnName();

                        this.addToMap(left_table, left_table_colName);
                        this.addToMap(right_table, right_table_colName);

                        /**change this if condition to support in mem and on disk thing**/
                        if (Main.inMem) {
                            dfsJoinChanges(iterator, left_table, right_table, left_table_colName, right_table_colName,
                                    null, whereList.get(i), ONEPASS, LEFT);
                        } else if (Main.onDisk) {
                            dfsJoinChanges(iterator, left_table, right_table, left_table_colName, right_table_colName,
                                    null, whereList.get(i), SORTMERGE, LEFT);
                        }

                    } else {


                        if (((EqualsTo) whereList.get(i)).getLeftExpression() instanceof BinaryExpression) {
                            Expression binaryExpression = ((EqualsTo) whereList.get(i)).getLeftExpression();
                            while (binaryExpression instanceof BinaryExpression) {
                                binaryExpression = ((BinaryExpression) binaryExpression).getLeftExpression();
                            }


                            Column column = (Column) (binaryExpression);
                            String col_name = column.getColumnName();
                            String table_name = column.getTable().getName();
                            dfs(iterator, table_name, null, whereList.get(i), LEFT);

                        } else {
                            Column column = ((Column) ((EqualsTo) whereList.get(i)).getLeftExpression());
                            String col_name = column.getColumnName();
                            String table_name = column.getTable().getName();
                            dfs(iterator, table_name, null, whereList.get(i), LEFT);
                        }
                    }
                } else if (whereList.get(i) instanceof InverseExpression) {

                    InverseExpression inverseExpression = (InverseExpression) whereList.get(i);
                    BinaryExpression expression2 = (BinaryExpression) inverseExpression.getExpression();
                    Column col = (Column) expression2.getLeftExpression();
                    String table_name = col.getTable().getName();
                    dfs(iterator, table_name,null, whereList.get(i), LEFT);

                } else {
                    //sort merge join if left and right are columns
                    if ((((BinaryExpression) whereList.get(i)).getLeftExpression() instanceof Column) &&
                            (((BinaryExpression) whereList.get(i)).getRightExpression() instanceof Column)) {
                        String col_name= "";
                        String table_name = "";

                        if (whereList.get(i) instanceof GreaterThan) {
                            Column column = ((Column) ((GreaterThan) whereList.get(i)).getLeftExpression());
                            col_name = column.getColumnName();
                            table_name = column.getTable().getName();

                        } else if (whereList.get(i) instanceof GreaterThanEquals) {
                            Column column = ((Column) ((GreaterThanEquals) whereList.get(i)).getLeftExpression());
                            col_name = column.getColumnName();
                            table_name = column.getTable().getName();

                        } else if (whereList.get(i) instanceof MinorThan) {
                            Column column = ((Column) ((MinorThan) whereList.get(i)).getLeftExpression());
                            col_name = column.getColumnName();
                            table_name = column.getTable().getName();

                        } else if (whereList.get(i) instanceof MinorThanEquals) {
                            Column column = ((Column) ((MinorThanEquals) whereList.get(i)).getLeftExpression());
                            col_name = column.getColumnName();
                            table_name = column.getTable().getName();
                        } else if (whereList.get(i) instanceof AndExpression) {
                            Column column = new Column();
                            if (whereList.get(i) instanceof MinorThanEquals) {
                                column = ((Column) ((MinorThanEquals) whereList.get(i)).getLeftExpression());
                            } else if (whereList.get(i) instanceof MinorThan) {
                                column = ((Column) ((MinorThan) whereList.get(i)).getLeftExpression());
                            } else if (whereList.get(i) instanceof GreaterThanEquals) {
                                column = ((Column) ((MinorThan) whereList.get(i)).getLeftExpression());
                            } else if (whereList.get(i) instanceof GreaterThan) {
                                column = ((Column) ((MinorThan) whereList.get(i)).getLeftExpression());
                            }

                            table_name = column.getTable().getName();
                        }

                        //String table_name = Main.CtoT.get(col_name);
                        //System.out.println("calling dfs for "+whereList.get(i));
                        dfs(iterator, table_name,null, whereList.get(i), LEFT);

                    } else {
                        String col_name= "";
                        String table_name = "";

                        if (whereList.get(i) instanceof GreaterThan) {
                            Column column = ((Column) ((GreaterThan) whereList.get(i)).getLeftExpression());
                            col_name = column.getColumnName();
                            table_name = column.getTable().getName();
                        } else if (whereList.get(i) instanceof GreaterThanEquals) {
                            Column column = ((Column) ((GreaterThanEquals) whereList.get(i)).getLeftExpression());
                            col_name = column.getColumnName();
                            table_name = column.getTable().getName();
                        } else if (whereList.get(i) instanceof MinorThan) {
                            Column column = ((Column) ((MinorThan) whereList.get(i)).getLeftExpression());
                            col_name = column.getColumnName();
                            table_name = column.getTable().getName();
                        } else if (whereList.get(i) instanceof MinorThanEquals) {
                            Column column = ((Column) ((MinorThanEquals) whereList.get(i)).getLeftExpression());
                            col_name = column.getColumnName();
                            table_name = column.getTable().getName();
                        } else if (whereList.get(i) instanceof OrExpression) {
                           col_name = "SHIPMODE";
                           table_name = Main.CtoT.get(col_name);
                        } else if (whereList.get(i) instanceof AndExpression) {
                            Column column = new Column();
                            BinaryExpression expression2 = (BinaryExpression) whereList.get(i);

                            if (expression2.getLeftExpression() instanceof MinorThanEquals) {
                                column = ((Column) ((MinorThanEquals) expression2.getLeftExpression()).getLeftExpression());
                            } else if (expression2.getLeftExpression() instanceof MinorThan) {
                                column = ((Column) ((MinorThan) expression2.getLeftExpression()).getLeftExpression());
                            } else if (expression2.getLeftExpression() instanceof GreaterThanEquals) {
                                column = ((Column) ((GreaterThanEquals) expression2.getLeftExpression()).getLeftExpression());
                            } else if (expression2.getRightExpression() instanceof GreaterThan) {
                                column = ((Column) ((GreaterThan) expression2.getLeftExpression()).getLeftExpression());
                            } else if (expression2.getRightExpression() instanceof MinorThan) {
                                column = ((Column) ((MinorThan) expression2.getRightExpression()).getLeftExpression());
                            } else if (expression2.getRightExpression()instanceof GreaterThanEquals) {
                                column = ((Column) ((GreaterThanEquals) expression2.getRightExpression()).getLeftExpression());
                            } else if (expression2.getRightExpression() instanceof GreaterThan) {
                                column = ((Column) ((GreaterThan) expression2.getRightExpression()).getLeftExpression());
                            } else if (expression2.getRightExpression() instanceof MinorThanEquals) {
                                column = ((Column) ((MinorThanEquals) expression2.getRightExpression()).getLeftExpression());
                            }

                            table_name = column.getTable().getName();
                        }

                        dfs(iterator, table_name,null, whereList.get(i), LEFT);
                    }
                }
            }
            iterator.leftChild = initialFilterIterator.leftChild;
        }
    }



    public void addToMap(String tableName, String column) {
        if (this.colsMap.containsKey(tableName)) {
            HashSet<String> l = colsMap.get(tableName);
            l.add(column);
            this.colsMap.put(tableName, l);
        } else {
            HashSet<String> l = new HashSet<>();
            l.add(column);
            this.colsMap.put(tableName, l);
        }
    }
}
