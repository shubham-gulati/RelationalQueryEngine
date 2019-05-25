package dubstep.Planner;

import dubstep.CustomClasses.CustomPair;
import dubstep.Iterators.*;
import dubstep.Main;
import dubstep.Optimizer.Optimizer;
import dubstep.Row.Row;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class Planner {
    Map<String, ArrayList<String>> colReqd;
    Expression deleteExpression=null;
    public Projection build(PlainSelect plainSelect, String alias2) throws IOException, IllegalAccessException, SQLException {

        String Alias2 = alias2;
        List<SelectItem> selectItemsList = plainSelect.getSelectItems();
        List<ScanIterator> scanIteratorsList = new ArrayList<>();


        boolean aggregate = false;

        FromItem fromItem1 = plainSelect.getFromItem();
        List<Join> lstJoin = plainSelect.getJoins();

        colReqd(selectItemsList,plainSelect.getWhere(),plainSelect.getGroupByColumnReferences(),fromItem1,lstJoin);

        Expression finalExpression=null;

        if(plainSelect.getWhere()!=null&&this.deleteExpression!=null){
            AndExpression andExpression = new AndExpression();
            andExpression.setLeftExpression(plainSelect.getWhere());
            andExpression.setRightExpression(this.deleteExpression);
            finalExpression=andExpression;
        }
        else if(this.deleteExpression!=null){
            finalExpression=this.deleteExpression;
        }
        else if(plainSelect.getWhere()!=null){
            finalExpression=plainSelect.getWhere();}


        for (SelectItem s : selectItemsList) {
            if (s instanceof SelectExpressionItem) {
                if (((SelectExpressionItem) s).getExpression() instanceof Function) {
                    aggregate = true;
                    break;
                }
            }
        }

        if (!aggregate) {
            if (plainSelect.getGroupByColumnReferences() != null)
                aggregate = true;
        }

        FromItem fromItem = plainSelect.getFromItem();
        FilterIterator filterIterator;

        if (fromItem instanceof Table) {
            Table table = (Table) fromItem;

            ArrayList<Row> temp = Main.insertMap.get(table.getName());
            ArrayList<Row> rowList = new ArrayList<>();
            if(temp!=null) {
                for (Row r : temp) {
                    rowList.add(r);
                }
            }

            if (table.getAlias() != null) {
                Main.aliases.put(table.getAlias(), table.getName());
                ArrayList<CustomPair<String, String>> tempTable = Main.coltoDt.get(table.getName());
                Main.coltoDt.put(table.getAlias(), tempTable);
                scanIteratorsList.add(new ScanIterator(table.getName(), table.getAlias(),colReqd.get(table.getName()),rowList));

            } else {
                Main.aliases.put(table.getName(), table.getName());
                scanIteratorsList.add(new ScanIterator(table.getName(), table.getName(),colReqd.get(table.getName()),rowList));
            }

        } else if (fromItem instanceof SubSelect) {

            if (fromItem.getAlias() != null) {
                Main.aliases.put(fromItem.getAlias(), fromItem.getAlias());

                try {
                    Main.SelectMethod(((SubSelect) fromItem).getSelectBody(), fromItem.getAlias());

                } catch (SQLException e) {
                    e.printStackTrace();
                }
                scanIteratorsList.add(new ScanIterator(fromItem.getAlias(), fromItem.getAlias(),colReqd.get(fromItem.getAlias()),Main.insertMap.get(fromItem.getAlias())));


            } else {
                String aliastemp = "abcd" + Main.counterAlias++;
                fromItem.setAlias(aliastemp);
                Main.aliases.put(aliastemp, aliastemp);

                try {
                    Main.SelectMethod(((SubSelect) fromItem).getSelectBody(), aliastemp);

                } catch (SQLException e) {
                    e.printStackTrace();
                }

                scanIteratorsList.add(new ScanIterator(aliastemp, aliastemp,colReqd.get(aliastemp),Main.insertMap.get(aliastemp)));

            }
        } else {
            throw new IllegalAccessException("Error");
        }

        if (plainSelect.getJoins() != null) {
            List<Join> joinItems = plainSelect.getJoins();
            for (Join j : joinItems) {
                if (j.getRightItem() instanceof Table) {
                    Table table = (Table) j.getRightItem();

                    ArrayList<Row> temp = Main.insertMap.get(table.getName());
                    ArrayList<Row> rowList = new ArrayList<>();
                    if(temp!=null) {
                        for (Row r : temp) {
                            rowList.add(r);
                        }
                    }

                    if (table.getAlias() != null) {
                        Main.aliases.put(table.getAlias(), table.getName());
                        ArrayList<CustomPair<String, String>> tempTable = Main.coltoDt.get(table.getName());
                        Main.coltoDt.put(table.getAlias(), tempTable);
                        scanIteratorsList.add(new ScanIterator(table.getName(), table.getAlias(),colReqd.get(table.getName()),rowList));
                    } else {
                        Main.aliases.put(table.getName(), table.getName());
                        scanIteratorsList.add(new ScanIterator(table.getName(), table.getName(),colReqd.get(table.getName()),rowList));
                    }
                }

                if (j.getRightItem() instanceof SubSelect) {
                    if (j.getRightItem().getAlias() != null) {
                        Main.aliases.put(j.getRightItem().getAlias(), j.getRightItem().getAlias());

                        try {
                            Main.SelectMethod(((SubSelect) j.getRightItem()).getSelectBody(), j.getRightItem().getAlias());
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                        scanIteratorsList.add(new ScanIterator(j.getRightItem().getAlias(), j.getRightItem().getAlias(),colReqd.get(j.getRightItem().getAlias()),Main.insertMap.get(j.getRightItem().getAlias())));
                    }
                }
            }
        }

        List<NestedLoopJoinIterator> NLJIteratorList = new ArrayList<>();

        if (scanIteratorsList.size() > 1) {
            NLJIteratorList.add(new NestedLoopJoinIterator(scanIteratorsList.get(0), scanIteratorsList.get(1)));
        }

        if (scanIteratorsList.size() > 2) {
            for (int i = 2; i < scanIteratorsList.size(); i++) {
                NLJIteratorList.add(new NestedLoopJoinIterator(NLJIteratorList.get(i - 2), scanIteratorsList.get(i)));
            }
        }


        Projection projectionIterator = new Projection(selectItemsList, aggregate, Alias2);
        if (scanIteratorsList.size() > 1) {
              if (finalExpression != null) {
                filterIterator = new FilterIterator(finalExpression, NLJIteratorList.get(NLJIteratorList.size() - 1));
                projectionIterator.leftChild = filterIterator;
                filterIterator.leftChild = NLJIteratorList.get(NLJIteratorList.size() - 1);
            } else
                projectionIterator.leftChild = NLJIteratorList.get(NLJIteratorList.size() - 1);
        } else {
            if (finalExpression != null) {
                filterIterator = new FilterIterator(finalExpression, scanIteratorsList.get(0));
                projectionIterator.leftChild = filterIterator;
            } else {
                projectionIterator.leftChild = scanIteratorsList.get(0);
            }
        }

        projectionIterator.setMap();

        /**Testing Code for Optimizer, query plan printing**/

        //Main.Printer(projectionIterator);

        Optimizer op = new Optimizer();
        op.reBuildTree(projectionIterator);
//        Main.Printer(projectionIterator);
        //System.exit(1);
        colReqd=null;
        return projectionIterator;
    }

    public void colReqd(List<SelectItem> selectItemsList,Expression whereCondition,List<Column> groupList,FromItem fromItem,List<Join> lstJoin){

        this.colReqd = new HashMap<>();

        //Select Item List
        for (SelectItem s : selectItemsList) {

            if(s instanceof AllColumns){
                if(fromItem instanceof Table){
                    tableColumns((Table)fromItem);
                }
                if(lstJoin!=null){
                    for(Join join:lstJoin){
                        if (join.getRightItem() instanceof Table) {
                            Table table = (Table) join.getRightItem();
                            tableColumns(table);
                        }
                    }
                }
            }
            else {
                Expression expression = ((SelectExpressionItem) s).getExpression();

                if (expression instanceof Column) {
                    addToMap((Column) expression);
                } else if (expression instanceof BinaryExpression) {
                    BinaryEval(expression);
                } else if (expression instanceof Function) {

                    if(((Function) expression).isAllColumns()){

                    }
                    else {
                        Expression expression2 = ((Function) expression).getParameters().getExpressions().get(0);

                        if(expression2 instanceof CaseExpression){
                            CaseExpression caseExpression = (CaseExpression)expression2;
                            Expression whenExpression = caseExpression.getWhenClauses().get(0).getWhenExpression();
                            AndOrEval(whenExpression);
                        }

                        else if (expression2 instanceof Column) {
                            addToMap((Column) expression2);
                        } else if (expression2 instanceof BinaryExpression) {
                            BinaryEval(expression2);

                        }
                    }
                }
            }
        }

        //Where List
        if(whereCondition!=null) {
            AndOrEval(whereCondition);
        }

        //Group by list
        if(groupList!=null) {
            for (Column column : groupList) {
                addToMap(column);
            }
        }

        //DeleteMap
        if(fromItem instanceof Table){
            String tableName = ((Table) fromItem).getName();
            if(Main.deleteMap.containsKey(tableName)){
                Expression expression = Main.deleteMap.get(tableName);

                //Updating deleteExpression
                if(this.deleteExpression==null){
                    this.deleteExpression=expression;
                }
                else{
                    AndExpression andExpression = new AndExpression();
                    andExpression.setLeftExpression(this.deleteExpression);
                    andExpression.setRightExpression(expression);
                    this.deleteExpression=andExpression;
                }

                AndOrEval(expression);
            }
        }
        if(lstJoin!=null){
            for(Join join:lstJoin){
                if (join.getRightItem() instanceof Table) {
                    String tableName = ((Table) join.getRightItem()).getName();
                    if(Main.deleteMap.containsKey(tableName)){
                        Expression expression = Main.deleteMap.get(tableName);


                        //Updating deleteExpression
                        if(this.deleteExpression==null){
                            this.deleteExpression=expression;
                        }
                        else{
                            AndExpression andExpression = new AndExpression();
                            andExpression.setLeftExpression(this.deleteExpression);
                            andExpression.setRightExpression(expression);
                            this.deleteExpression=andExpression;
                        }

                        AndOrEval(expression);
                    }
                }
            }
        }

        //UpdateMap
        if(fromItem instanceof Table){
            String tableName = ((Table) fromItem).getName();
            if(Main.updateExpressionMap.containsKey(tableName)){
                Expression expression = Main.updateExpressionMap.get(tableName);
                AndOrEval(expression);
            }
        }
        if(lstJoin!=null){
            for(Join join:lstJoin){
                if (join.getRightItem() instanceof Table) {
                    String tableName = ((Table) join.getRightItem()).getName();
                    if(Main.updateExpressionMap.containsKey(tableName)){
                        Expression expression = Main.updateExpressionMap.get(tableName);
                        AndOrEval(expression);
                    }
                }
            }
        }
    }

    public void addToMap(Column column){
        String tableName=column.getTable().getName();

        if (tableName != null) {
            tableName=column.getTable().getName();
        } else {
            tableName=Main.CtoT.get(column.getColumnName());
        }
        if(colReqd.containsKey(tableName)){

            ArrayList<String> arrayList = colReqd.get(tableName);
            if(!(arrayList.contains(column.getColumnName()))){
                arrayList.add(column.getColumnName());
                colReqd.put(tableName,arrayList);
            }

        }
        else{
            ArrayList<String> arrayList = new ArrayList();
            arrayList.add(column.getColumnName());
            colReqd.put(tableName,arrayList);
        }
    }

    public void BinaryEval(Expression expression){
        if(expression instanceof BinaryExpression){
            BinaryExpression binaryExpression = (BinaryExpression)expression;
            BinaryEval(binaryExpression.getLeftExpression());
            BinaryEval(binaryExpression.getRightExpression());
        }
        else if(expression instanceof Column){
            addToMap((Column)expression);
        }
    }

    public void AndOrEval(Expression expression){
        if(expression instanceof InverseExpression){
            expression = ((InverseExpression)expression).getExpression();
        }

        if((expression instanceof AndExpression)||(expression instanceof OrExpression)){
            BinaryExpression binaryExpression = (BinaryExpression)expression;
            AndOrEval(binaryExpression.getLeftExpression());
            AndOrEval(binaryExpression.getRightExpression());
        }
        else{
            BinaryEval(expression);
        }
    }

    public void tableColumns(Table table){
        ArrayList<CustomPair<String,String>> temp=Main.coltoDt.get(table.getName());
        ArrayList<String> temp1 = new ArrayList<>();

        for(CustomPair<String,String> customPair:temp){
            temp1.add(customPair.getFirst());
        }
        colReqd.put(table.getName(),temp1);
    }
}
