package dubstep.Optimizer;

import dubstep.CustomClasses.CustomPair;
import dubstep.Iterators.ScanIterator;
import dubstep.Main;
import dubstep.Row.Row;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import java.io.*;
import java.util.*;

/**Author : Shubham Gulati **/


public class ExternalSort {

    static int M = 1000;
    PriorityQueue<Row> pq;
    String fileName;
    String tempFileName = "temp_file_";
    public String finalFileName;
    public int counter = 0;
    public List<Column> list_columns;
    public boolean flag = false;
    public String tableNme;
    Map<String, CustomPair<Integer, String>> tempMap;
    boolean scanFlag;

    public ExternalSort(String name, List<Column> orderList, boolean flag, Map<String, CustomPair<Integer, String>> mp2,
                        boolean scanFlag) {
       // System.out.println("sorting file "+name);
        this.tableNme = name;
        this.fileName = name;
        this.list_columns = orderList;
        this.flag = flag;
        this.tempMap = mp2;
        this.scanFlag = scanFlag;

       // System.out.println("sorting on "+this.list_columns.get(0).toString());

        //System.out.println("filename "+this.fileName);
        //System.out.println("map size "+this.tempMap.size());
        //System.out.println("list column "+this.list_columns.size());

        if (!this.flag) {
            pq = new PriorityQueue(M, new OrderByComparator());
        } else {
            pq = new PriorityQueue(M, new NormalComparator(this.list_columns, this.tempMap));
        }
    }

    //takes 1000 tuples from priority queue and write to file and then write the file onto disk
    public void writeToTempFile() throws IOException {
        FileWriter fw = new FileWriter("tempFolder/"+tempFileName + counter + ".csv");
        PrintWriter pw = new PrintWriter(fw);
        while (pq.size() > 0) {
            Row s = pq.poll();
            String vl = fromRow(s);
            //System.out.println("writing value "+vl);
            pw.println(vl);
        }
        pw.flush();
        pw.close();
        fw.close();
    }

    public void sortBigFile() {
        //System.out.println("sorting big file "+this.fileName);
        try {
            //System.out.println("this file name is "+this.fileName);
            FileReader fr;
            if (this.scanFlag) {
                //System.out.println("inside reader from data "+this.fileName);
//                fr = new FileReader("/Users/shubham/DB/data/"+this.fileName+".csv");
                fr = new FileReader("data/"+this.fileName+".csv");
            } else {
                //System.out.println("inside reader from temp "+this.fileName);
                fr = new FileReader("tempFolder/" + this.fileName + ".csv");
            }

            BufferedReader br = new BufferedReader(fr);
            String t = "";
            int local_counter = 0;


            while ((t = br.readLine()) != null) {
                if (local_counter < M) {
                    Row r = getRow(t);
                    pq.add(r);
                    local_counter++;
                } else {
                    Row r = getRow(t);
                    pq.add(r);
                    writeToTempFile();
                    pq.clear();
                    local_counter = 0;
                    counter++;
                }
            }

            if (pq.size() > 0) {
                writeToTempFile();
                pq.clear();
                counter++;
            }
            //now we need to merge all the sorted files. now we read first element of each file and pick the minimum one
            //and move to next in that file, similarly for all other files

            fr.close();
            br.close();
            mergeFilesInitialize(counter);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private void mergeFilesInitialize(int num) throws FileNotFoundException {
        try {
            ArrayList<FileReader> listFileReader = new ArrayList<>();
            ArrayList<BufferedReader> listBufferReader = new ArrayList<>();

            for (int i = 0; i < num; i++) {
                String file = "tempFolder/"+tempFileName + i + ".csv";
                listFileReader.add(new FileReader(file));
                listBufferReader.add(new BufferedReader(listFileReader.get(i)));
            }
            sortFilesWriteFinalOutput(listBufferReader);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public void sortFilesWriteFinalOutput(List<BufferedReader> list) {
        try {
            PriorityQueue<Row> priorityQueue;

            if (flag) {
                priorityQueue =  new PriorityQueue(M, new NormalComparator(this.list_columns, this.tempMap));
            } else {
                priorityQueue = new PriorityQueue<Row>(M, new OrderByComparator());
            }

            for (int i=0; i<list.size(); i++) {
                String l = list.get(i).readLine();
                if (l != null) {
                    Row f = getRow(l);
                    f.indexRow = i;
                    priorityQueue.add(f);
                }
            }

            FileWriter fw;
            fw = new FileWriter("tempFolder/" + this.fileName + ".csv");
            //System.out.println("writing file "+this.fileName);
            PrintWriter pw = new PrintWriter(fw);

            while (true) {
                if (priorityQueue.size() == 0) {
                    break;
                } else {
                    Row intermediate = priorityQueue.peek();
                    String result =  fromRow(priorityQueue.poll());
//                    System.out.println("writing final result "+result);
                    pw.println(result);
                    int ind = intermediate.indexRow;

                    String newLine = list.get(ind).readLine();
                    if (newLine != null) {
                        Row f1 = getRow(newLine);
                        f1.indexRow = ind;
                        priorityQueue.add(f1);
                    } else {
                        //now current file finished, need to check further files now
                        for (int j=0; j<list.size(); j++) {
                            if (j == ind) {
                                continue;
                            } else {
                                String data = list.get(j).readLine();
                                if (data != null) {
                                    Row f2 = getRow(data);
                                    f2.indexRow = j;
                                    priorityQueue.add(f2);
                                }
                            }
                        }
                    }
                }
            }
            pw.flush();
            pw.close();
            fw.close();
            priorityQueue.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String fromRow(Row r) {
        String res = "";
        List<PrimitiveValue> rr = r.RowValues;
        int cn = 0;
        for (int i=0;i<rr.size();i++) {
            String val = rr.get(i).toRawString();
            res+=val;
            if (cn < rr.size() - 1) {
                res += "|";
            }
            cn++;
        }
        return res;
    }

    public Row getRow(String data) {

        if (data != null) {
            String[] parts = data.split("|");
            List<PrimitiveValue> RowMap = new ArrayList<>();
            String colName = "";
            String type = "";

            if (flag) {
                //System.out.println("inside");
                ArrayList<CustomPair<String, String>> arrayList = Main.coltoDt.get(tableNme);
                //System.out.println("parts length is "+parts.length);
                for (int i=0;i<parts.length;i++) {
                    colName = arrayList.get(i).getFirst();
                    colName = tableNme+"."+colName;
                    type = arrayList.get(i).getSecond();

                    if (type.equals("int")) {
                        Long l = Long.parseLong(parts[i]);
                        LongValue l1 = new LongValue(l);
                        RowMap.add(l1);
                    } else if (type.equals("string") || type.equals("varchar") || type.equals("char")) {
                        StringValue s1 = new StringValue(parts[i]);
                        RowMap.add(s1);
                    } else if (type.equals("decimal")) {
                        Double d = Double.parseDouble(parts[i]);
                        DoubleValue d1 = new DoubleValue(d);
                        RowMap.add(d1);
                    } else if (type.equals("date")) {
                        DateValue dateValue = new DateValue(parts[i]);
                        RowMap.add(dateValue);
                    }
                }


            } else {
                for (int i = 0; i < parts.length; i++) {
                    SelectItem selectItem = Main.items.get(i);
                    //System.out.println(selectItem.toString());

                    if (selectItem instanceof SelectExpressionItem) {
                        if (((SelectExpressionItem) selectItem).getExpression() instanceof BinaryExpression) {

                            SelectExpressionItem selectExpressionItem = ((SelectExpressionItem) selectItem);
                            //System.out.println("binary expression");
                            Expression expression = ((BinaryExpression) selectExpressionItem.getExpression()).getLeftExpression();
                            while (expression instanceof BinaryExpression) {
                                expression = ((BinaryExpression) expression).getLeftExpression();
                            }
                            colName = expression.toString();

                        } else if (((SelectExpressionItem) selectItem).getExpression() instanceof Function) {
                            //handle cases of two, three parameters in a aggregate
                            //System.out.println("function");
                            colName = ((Function) selectItem).getParameters().getExpressions().get(0).toString();
                            //System.out.println(colName);
                            //System.exit(1);
                        } else {
                            //System.out.println("in else");
                            colName = selectItem.toString();
                        }
                    }

                    String tableName = Main.CtoT.get(colName);
                    ArrayList<CustomPair<String, String>> arrayList = Main.coltoDt.get(tableName);

                    for (int j = 0; j < arrayList.size(); j++) {
                        if (arrayList.get(j).getFirst().equals(colName)) {
                            type = arrayList.get(j).getSecond().toLowerCase();
                        }
                    }

                    String[] typesBreak = type.split("\\ ");
                    if (typesBreak.length > 1) {
                        type = typesBreak[0];
                    }

                    //System.out.println("colname is "+colName+ "type is "+type+" data"+parts[i]);
                    if (type.equals("int")) {
                        Long l = Long.parseLong(parts[i]);
                        LongValue l1 = new LongValue(l);
                        RowMap.add(l1);
                    } else if (type.equals("string") || type.equals("varchar") || type.equals("char")) {
                        StringValue s1 = new StringValue(parts[i]);
                        RowMap.add(s1);
                    } else if (type.equals("decimal")) {
                        Double d = Double.parseDouble(parts[i]);
                        DoubleValue d1 = new DoubleValue(d);
                        RowMap.add(d1);
                    } else if (type.equals("date")) {
                        DateValue dateValue = new DateValue(parts[i]);
                        RowMap.add(dateValue);
                    }
                }
            }

            Row r = new Row();
            r.RowValues = RowMap;
            return r;
        }
        return null;
    }
}
