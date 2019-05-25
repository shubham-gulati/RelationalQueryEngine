package dubstep;

import dubstep.CustomClasses.CustomPair;
import dubstep.Eval.EvalCustom;
import dubstep.Iterators.*;
import dubstep.Iterators.Iterator;
import dubstep.Optimizer.*;
import dubstep.Planner.Planner;
import dubstep.Row.Row;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;

public class Main {
    public static Map<String, ArrayList<CustomPair<String, String>>> coltoDt = new HashMap<>();
    public static Map<String, String> CtoT = new HashMap<>();
    public static HashMap<String, String> aliases = new HashMap<>();
    public static String fileName = "external-sort.csv";
    public static boolean doOrderBy = false;
    public static List<OrderByElement> orderByElements = new ArrayList<>();
    public static List<SelectItem> items = new ArrayList<>();
    public static Limit limit;
    public static long limit_count = -1;
    public static boolean inMem = true;
    public static boolean onDisk = false;
    public static int counterAlias = 0;
    public static ArrayList<ArrayList<Row>> rrr;
    public static int crr = -1;
    public static Pattern pattern;
    public static boolean indexesCreateSetup = false;
    public static Map<String,ArrayList<Row>> insertMap = new HashMap<>();
    public static Map<String,Expression> deleteMap = new HashMap<>();
    public static Map<String,Expression> updateExpressionMap = new HashMap<>();
    public static Map<String,ArrayList<CustomPair<String,CaseExpression>>> updateCaseMap = new HashMap<>();
    public static boolean csvTuple = true;

    public static void Printer(Projection p1) {
        Queue<Iterator> queue = new LinkedList<>();
        queue.add(p1);
        while (queue.size() != 0) {

            if (queue.peek().leftChild != null)
                queue.add(queue.peek().leftChild);

            if (queue.peek().rightChild != null)
                queue.add(queue.peek().rightChild);


            Iterator iterator = queue.peek();
            if (iterator instanceof ScanIterator) {
                System.out.println("Scan " + ((ScanIterator) iterator).getTableName());
                System.out.println("left child  list is " + iterator.iteratorLeftList);
            }

            if (iterator instanceof Projection) {
                System.out.println("left child  is " + iterator.leftChild.getClass().toString());
            }

            if (iterator instanceof FilterIterator) {
                System.out.println("Filter " + ((FilterIterator) iterator).getExpression());
                System.out.println("leftchild is "+iterator.leftChild.getClass().toString());
                System.out.println("left child  list is " + iterator.iteratorLeftList);
            }

            if (iterator instanceof NestedLoopJoinIterator) {
                System.out.println("NLP");
                System.out.println("left list is " + iterator.iteratorLeftList);
                System.out.println("right list is " + iterator.iteratorRightList);
            }

            if (iterator instanceof OnePasHashJoin) {
                System.out.println("One Pass Hash");
                System.out.println(((OnePasHashJoin) iterator).getExpression());
                System.out.println("left list is " + iterator.iteratorLeftList);
                System.out.println("right list is " + iterator.iteratorRightList);
            }
            if (iterator instanceof SortMergeJoin) {
                System.out.println("Sort merge join");
                //System.out.println(((SortMergeJoin) iterator).getExpression());
                System.out.println("left list is "+iterator.iteratorLeftList);
                System.out.println("right list is "+iterator.iteratorRightList);
            }

            if (iterator instanceof IndexNestedLoopJoin) {
                System.out.println("Index Nested Loop join");
                //System.out.println(((IndexNestedLoopJoin) iterator).ge);
                System.out.println("left list is " + iterator.iteratorLeftList);
                System.out.println("right list is " + iterator.iteratorRightList);
            }

            queue.remove();
        }
        System.out.println("***************Done*********************");
    }

    public static void SelectMethod(SelectBody selectBody, String alias2) throws IOException, IllegalAccessException, SQLException {

        rrr = new ArrayList<>();
        limit_count = -1;
        List<Row> rowList;
        FileWriter ext = null;
        FileOutputStream out = null;

        if (selectBody instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) selectBody;

            if (plainSelect.getSelectItems() != null) {
                items.clear();
                items.addAll(plainSelect.getSelectItems());
            }

            if (plainSelect.getLimit() != null) {
                limit = plainSelect.getLimit();
                limit_count = limit.getRowCount();
            }

            if (plainSelect.getOrderByElements() != null) {
                orderByElements.clear();
                orderByElements.addAll(plainSelect.getOrderByElements());
                doOrderBy = true;
            }

            if (doOrderBy && onDisk) {
                ext = new FileWriter("tempFolder/" + fileName);
            }

            Planner planner = new Planner();
            Projection projection = planner.build(plainSelect, alias2);

            //out = new FileOutputStream("/Users/adityaagarwal/Documents/Spring 2019/DBMS/Queries/NBA_Examples/" + p.Alias2 + ".dat");
            if (projection.Alias2 != null) out = new FileOutputStream("/Users/shubham/DB/data/" + projection.Alias2 + ".csv");

            if (projection.aggregate) {
                Aggregate aggregate= new Aggregate(plainSelect,projection);
                if (plainSelect.getGroupByColumnReferences() != null) {
                    rowList=aggregate.GroupBy();
                } else {
                    rowList=aggregate.AggWithoutGroupBy();
                }
            } else {
                rowList=new ArrayList<>();
                while (projection.hasNext()) {
                    Row row = projection.next();
                    if (row != null) {
                        rowList.add(row);
                    }
                }


            }


            if (doOrderBy) {
                if (inMem) {
                    Collections.sort(rowList, new OrderByComparator());
//                    if (p.Alias2 != null) {
//                        Map<String, PrimitiveValue> mp = rowList.get(0).RowValues;
//                        ArrayList<CustomPair<String, String>> temp = new ArrayList<>();
//                        for (String s : mp.keySet()) {
//
//                            temp.add(new CustomPair<>(s.substring(0, s.indexOf('.')), s.substring(s.indexOf('.') + 1)));
//                            if (!Main.CtoT.containsKey(s.substring(0, s.indexOf("."))))
//                                Main.CtoT.put(s.substring(0, s.indexOf('.')), p.Alias2);
//                        }
//                        Main.coltoDt.put(p.Alias2, temp);
//                        writeFile(rowList, limit_count, out);
//                    } else
                        printRows(rowList, limit_count);

                } else if (onDisk) {

//                    String fileNametemp = "";
//                    Iterator it = p.leftChild;
//                    while (it instanceof FilterIterator) {
//                        it = it.leftChild;
//                    }

                    writeFile(rowList, -1, ext);
                    ExternalSort externalSort = new ExternalSort(fileName, null, false, projection.mp, false);
                    externalSort.sortBigFile();

                    FileReader fr = new FileReader("tempFolder/"+fileName + ".csv");
                    BufferedReader br = new BufferedReader(fr);
                    String t;
                    int counter_limit = 0;


//                    if (p.Alias2 != null) {
//
//                        Map<String, PrimitiveValue> mp = rowList.get(0).RowValues;
//                        ArrayList<CustomPair<String, String>> temp = new ArrayList<>();
//                        for (String s : mp.keySet()) {
//
//                            temp.add(new CustomPair<>(s.substring(0, s.indexOf('.')), s.substring(s.indexOf('.') + 1)));
//                            if (!Main.CtoT.containsKey(s.substring(0, s.indexOf("."))))
//                                Main.CtoT.put(s.substring(0, s.indexOf('.')), p.Alias2);
//                        }
//                        Main.coltoDt.put(p.Alias2, temp);
//
//                        while ((t = br.readLine()) != null) {
//
//                            counter_limit++;
//                            out.write(t.getBytes());
//                            if (counter_limit == limit_count)
//                                break;
//                        }
//                    } else {

                        while ((t = br.readLine()) != null) {
                            counter_limit++;
                            System.out.println(t);
                            if (counter_limit == limit_count)
                                break;
                        }
//                    }
                }
            } else {
//                if (p.Alias2 != null) {
//                    Map<String, PrimitiveValue> mp = rowList.get(0).RowValues;
//                    ArrayList<CustomPair<String, String>> temp = new ArrayList<>();
//                    for (String s : mp.keySet()) {
//
//                        temp.add(new CustomPair<>(s.substring(0, s.indexOf('.')), s.substring(s.indexOf('.') + 1)));
//                        if (!Main.CtoT.containsKey(s.substring(0, s.indexOf("."))))
//                            Main.CtoT.put(s.substring(0, s.indexOf('.')), p.Alias2);
//                    }
//                    Main.coltoDt.put(p.Alias2, temp);
//                    writeFile(rowList, limit_count, out);
//                } else

//                final long startTime1 = System.currentTimeMillis();
                    printRows(rowList, limit_count);
//                final long endTime1 = System.currentTimeMillis();
//                abcd =  (endTime1 - startTime1);
            }
        }
        rrr.clear();
        crr = -1;

    }

    public static void setupCreate() {
        try {
            File folder = new File("createCommands");
            File[] listOfFiles = folder.listFiles();

            for (int i=0;i<listOfFiles.length;i++) {
                String filename = listOfFiles[i].getName();
                FileReader fileReader = new FileReader("createCommands/"+filename);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                String line = bufferedReader.readLine();
                //System.out.println(line);
                CCJSqlParser ccjSqlParser = new CCJSqlParser(new StringReader(line));
                Statement query = null;

                try {
                    query = ccjSqlParser.Statement();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Exception during parsing!");
                }

                CreateTable createTable = (CreateTable) query;
                mappings(createTable);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void statementParser(Statement query, String alias, String writeToFile) throws IOException, IllegalAccessException, SQLException {


        if(query instanceof Update){
            Update update = (Update)query;
            List<Expression> lstValue=update.getExpressions();
            List<Column> lstCol = update.getColumns();
            String tableName = update.getTable().getName();
            Expression expression = update.getWhere();


            //Updating updateExpressionMap from where conditions to read all those columns as well
            if(!updateExpressionMap.containsKey(tableName)){
                updateExpressionMap.put(tableName,expression);}
            else{
                Expression expression1 = updateExpressionMap.get(tableName);
                AndExpression andExpression = new AndExpression();
                andExpression.setLeftExpression(expression1);
                andExpression.setRightExpression(expression);
                updateExpressionMap.put(tableName,andExpression);
            }
            /*****************************************************/

            for(int i=0;i<lstValue.size();i++){

                //Getting full column name on which case to be made
                Column colTemp =lstCol.get(i);

                String colName ="";
                if(colTemp.getTable().getName()!=null){
                    colName=colTemp.getWholeColumnName();
                }
                else{
                    colName = tableName+"."+colTemp.getColumnName();
                }
                /******************************************/



                //Building case expression for each
                WhenClause whenClause = new WhenClause();
                whenClause.setWhenExpression(expression);
                whenClause.setThenExpression(lstValue.get(i));
                List<WhenClause> lstE =new ArrayList<>();
                lstE.add(whenClause);

                CaseExpression caseExpression = new CaseExpression();
                caseExpression.setWhenClauses(lstE);
                caseExpression.setElseExpression(colTemp);
                /********************************************/

                //Updating the updateCaseMap Map

                if(!updateCaseMap.containsKey(tableName)){

                    ArrayList<CustomPair<String,CaseExpression>> temp = new ArrayList<>();
                    temp.add(new CustomPair<>(colName,caseExpression));
                    updateCaseMap.put(tableName,temp);

                }
                else{
                    ArrayList<CustomPair<String,CaseExpression>> temp = updateCaseMap.get(tableName);
                    temp.add(new CustomPair<>(colName,caseExpression));
                    updateCaseMap.put(tableName,temp);
                }

                /***********************************************/

            }
        }

        if(query instanceof Delete){

            if (!indexesCreateSetup) {
                setupCreate();
                indexesCreateSetup = true;
            }

            Delete delete = (Delete)query;
            Expression expression = delete.getWhere();
            BinaryExpression binaryExpression = (BinaryExpression)expression;
            Column column = (Column)binaryExpression.getLeftExpression();
            if(column.getTable().getName()==null){
                column.setTable(delete.getTable());
            }
            binaryExpression.setLeftExpression(column);
            expression=binaryExpression;
            InverseExpression inverseExpression = new InverseExpression(expression);
            String tableName= delete.getTable().getName();
            if(!deleteMap.containsKey(tableName)){
                  deleteMap.put(tableName,inverseExpression);

            }
            else{
                Expression expression1 = deleteMap.get(tableName);
                AndExpression andExpression = new AndExpression();
                andExpression.setLeftExpression(expression1);
                andExpression.setRightExpression(inverseExpression);
                deleteMap.put(tableName,andExpression);
            }

            if(insertMap.containsKey(tableName)){
                ArrayList<CustomPair<String, String>> temp = Main.coltoDt.get(tableName);
                HashMap<String, CustomPair<Integer,String>> mp = new HashMap<>();
                int i=0;
                for(CustomPair<String,String> customPair: temp){
                    String colName = tableName+"."+customPair.getFirst();
                    CustomPair<Integer,String > customPair1 = new CustomPair<>(i++,customPair.getSecond());
                    mp.put(colName,customPair1);
                }
                EvalCustom evalCustom = new EvalCustom(mp);
                ArrayList<Row> tempList = Main.insertMap.get(tableName);

                ArrayList<Row> tempList1 = new ArrayList<>();

                for(Row row:tempList){
                    if(evalCustom.evaluateWhere(row,inverseExpression)){
                        tempList1.add(row);
                    }
                }

                Main.insertMap.put(tableName,tempList1);

            }


        }

        if(query instanceof Insert){

            if (!indexesCreateSetup) {
                setupCreate();
                indexesCreateSetup = true;
            }

            List<PrimitiveValue> lstValues = new ArrayList<>();
            Row row = new Row();

            Insert insert = (Insert)query;
            String tableName = insert.getTable().getName();
            List<Column> lstColumn=insert.getColumns();
            ItemsList lstItem=insert.getItemsList();
            ExpressionList expressionList = (ExpressionList) lstItem;
            List<Expression> lstExpression = expressionList.getExpressions();

            for(Expression expression:lstExpression){
                lstValues.add((PrimitiveValue) expression);
            }

            row.RowValues = lstValues;

            if(!insertMap.containsKey(tableName)){
                ArrayList<Row> temp = new ArrayList<>();
                temp.add(row);
                insertMap.put(tableName,temp);
            }
            else{
                ArrayList<Row> temp = insertMap.get(tableName);
                temp.add(row);
            }
        }

        if (query instanceof Select) {

            if (!indexesCreateSetup) {
                setupCreate();
                indexesCreateSetup = true;
            }

            Select select = (Select) query;
            SelectBody selectBody = select.getSelectBody();
            SelectMethod(selectBody, alias);
        } else if (query instanceof CreateTable) {
            CreateTable createTable = (CreateTable) query;
            mappings(createTable);
            String tableName = createTable.getTable().getName();
            CacheCreate.cacheToDisk(tableName, writeToFile);

            ProjectionPushdownWrite projectionPushdownWrite = new ProjectionPushdownWrite(createTable.getTable().getName(), createTable.getColumnDefinitions());
            projectionPushdownWrite.writeToDisk();

        }
    }

    public static void mappings(CreateTable createTable) {
        String tableName = createTable.getTable().getName();
        List<ColumnDefinition> columnList = createTable.getColumnDefinitions();
        ArrayList<CustomPair<String, String>> l = new ArrayList<>();

        for (ColumnDefinition col : columnList) {
            String type = col.getColDataType().toString().toLowerCase();
            String[] type_breaks = type.split("\\ ");
            if (type_breaks.length > 1) {
                type = type_breaks[0];
            }
            CustomPair<String, String> pair = new CustomPair<>(col.getColumnName(), type);
            l.add(pair);
        }

        for (ColumnDefinition col : columnList)
            CtoT.put(col.getColumnName(), tableName);

        coltoDt.put(tableName, l);
    }

    public static Row next(String s, String tableName) {

        //how to more optimize this
        //compiling pattern only once
        String[] parts = pattern.split(s);

        ArrayList<CustomPair<String, String>> coltoDataList = Main.coltoDt.get(tableName);
        List<PrimitiveValue> data = new ArrayList<>();

        for (int i = 0; i < parts.length; i++) {
            String type = coltoDataList.get(i).getSecond().toLowerCase();

            //split function here
            String[] typesBreak = type.split("\\ ");

            if (typesBreak.length > 1) {
                type = typesBreak[0];
            }

            if (type.equals("int")) {
                Long l = Long.parseLong(parts[i]);
                LongValue l1 = new LongValue(l);
                data.add(l1);
            } else if (type.equals("string") || type.equals("varchar") || type.equals("char")) {
                StringValue s1 = new StringValue(parts[i]);
                data.add(s1);
            } else if (type.equals("decimal")) {
                Double d = Double.parseDouble(parts[i]);
                DoubleValue d1 = new DoubleValue(d);
                data.add(d1);
            } else if (type.equals("date")) {
                DateValue dateValue = new DateValue(parts[i]);
                data.add(dateValue);
            }
        }

        Row r = new Row();
        r.RowValues = data;
        return r;
    }

    public static void main(String[] args) throws IOException, IllegalAccessException, SQLException {

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--in-mem")) {
                inMem = true;
            } else if (args[i].equals("--on-disk")) {
                 onDisk = true;
            }
        }

        if (onDisk) {
            String filePath = "tempFolder";
            Path path = Paths.get(filePath);
            if (!Files.exists(path)) {
                Files.createDirectory(path);
            }
        }

        Scanner sc = new Scanner(System.in);
        do {
            System.out.println("$> ");
            String result = "";
            String val = "";

            while (!val.contains(";")) {
                val = sc.next();
                result += val;
                result += " ";
            }

            if (sc.hasNextLine()) {
                String line = "";
                if ((line = sc.nextLine()) != null) {
                    line = line.trim();
                    result += line;
                }
            }


            CCJSqlParser parser = new CCJSqlParser(new StringReader(result));
            Statement query = null;
            try {

                query = parser.Statement();

            } catch (Exception e) {
                System.out.println("Exception during parsing!");
            }
            statementParser(query, null, result);


        } while (true);
    }

    public static void printRows(List<Row> rowList, long limit_count) {

        int rowCount = 0;
        List<PrimitiveValue> rowIndex;
        for (Row row : rowList) {
            rowIndex = row.RowValues;
            if (rowIndex != null) {
                rowCount++;
                int count = 0;
                for (PrimitiveValue value : rowIndex) {
                    count++;
                    System.out.print(value.toRawString());
                    if (count != rowIndex.size())
                        System.out.print("|");
                    else
                        System.out.println();

                }
                if (rowCount == limit_count) {
                    break;
                }
            }
        }
        System.out.flush();
    }


    public static void writeFile(List<Row> rowList, long limit_count, FileWriter fileWriter) throws IOException {

        int rowCount = 0;
        List<PrimitiveValue> rowIndex;
        for (Row row : rowList) {
            rowIndex = row.RowValues;
            if (rowIndex != null) {
                rowCount++;
                int count = 0;
                for (PrimitiveValue value : rowIndex) {
                    count++;
                    fileWriter.write(value.toRawString());
                    if (count != rowIndex.size())
                        fileWriter.write("|");
                    else
                        fileWriter.write("\n");
                }
                if (rowCount == limit_count) {
                    break;
                }
            }
        }
    }

    public static String getcolDataType(String colName) {
        String TableName;
        String ActualColName;
        if (colName.contains(".")) {
            TableName = colName.substring(0, colName.indexOf("."));
            ActualColName = colName.substring(colName.indexOf(".") + 1);
        } else {
            TableName = Main.CtoT.get(colName);
            ActualColName = colName;
        }

        List<CustomPair<String, String>> temp = Main.coltoDt.get(TableName);

        for (CustomPair<String, String> customPair : temp) {
            if (customPair.getFirst().equals(ActualColName)) {
                return customPair.getSecond();
            }
        }
        return null;
    }
}
