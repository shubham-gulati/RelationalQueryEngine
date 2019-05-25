package dubstep.Iterators;

import dubstep.CustomClasses.CustomPair;
import dubstep.Main;
import dubstep.Optimizer.ExternalSort;
import dubstep.Row.Row;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.io.*;
import java.sql.SQLException;
import java.util.*;


public class SortMergeJoin extends Iterator {
    Iterator R;
    Iterator S;
    Iterator R_S;
    private BufferedReader bufferedReader;

    private Expression leftExpression=null;
    private String leftExpressionString = null;
    private String rightExpressionString = null;
    private  Expression rightExpression=null;
    private String leftTableName=null;
    private String rightTableName=null;
    int mark = 1;
    boolean flagReset = true;
    Row result = null;
    String line=null;
    boolean firstFlag=true;
    //FileOutputStream fileOutputStream;
    FileWriter fileWriter;
    PrintWriter pw;
    boolean flagForExternalSort = false;
    String temp_Name;
    ExternalSort eR;
    ExternalSort eS;
    List<Column> leftList;
    List<Column> rightList;
    Iterator tempr;
    Iterator templ;
    int cntt;
    boolean fggg = true;
    String[] ss;

    Row r = null;
    Row s = null;
    Row temps=null;

    public SortMergeJoin(Iterator R, Iterator S, Expression expression) throws IOException, SQLException {
        this.cntt=0;
        this.R = R;
        this.S = S;
        this.leftChild = R;
        this.rightChild = S;
//        r = this.R.next();
//        s = this.S.next();
        this.leftExpression = ((BinaryExpression) expression).getLeftExpression();
        this.rightExpression = ((BinaryExpression) expression).getRightExpression();
        //this.R_S = new ScanIterator(R+"_"+S,R+"_"+S);

        Column leftExpressionColumn = (Column) leftExpression;
        Column rightExpressionColumn = (Column) rightExpression;
        leftTableName =leftExpressionColumn.getTable().getName();
        if(leftTableName==null || leftTableName.equals(null)){
            leftTableName = Main.CtoT.get(leftExpressionColumn.toString());
            leftExpressionString = leftTableName + "."+leftExpressionColumn.getColumnName();
        }else{
            leftExpressionString = this.leftExpression.toString();
        }

        rightTableName =rightExpressionColumn.getTable().getName();
        if(rightTableName==null || rightTableName.equals(null)){
            rightTableName = Main.CtoT.get(rightExpressionColumn.toString());
            rightExpressionString = rightTableName + "."+rightExpressionColumn.getColumnName();
        }else{
            rightExpressionString = this.rightExpression.toString();
        }

        if ((this.leftChild.mp != null) && (this.rightChild.mp != null)) {
            this.mp = new HashMap<>();
            int size1 = this.leftChild.mp.size();
            int size2 = this.rightChild.mp.size();
            this.mp.putAll(this.leftChild.mp);
            Map<String, CustomPair<Integer, String>> temp = new HashMap<>();

            for(Map.Entry<String, CustomPair<Integer, String>> entry: this.rightChild.mp.entrySet()) {
                CustomPair<Integer, String> val = entry.getValue();
                CustomPair<Integer, String> newVal = new CustomPair<>(val.getFirst() + size1, val.getSecond());
                temp.put(entry.getKey(), newVal);
            }

            this.mp.putAll(temp);
        }

        //to correct left right order of coming expression
        String tempExpression="";

        this.ss =this.rightChild.toString().split("\\.");
        //System.out.println(ss[0]);
//        if (!(this.S.toString().equals(this.rightTableName))) {

        if (!(ss[0].equals(this.rightTableName))) {
            tempExpression = this.leftExpressionString;
            this.leftExpressionString = this.rightExpressionString;
            this.rightExpressionString = tempExpression;
            this.flagForExternalSort = true;
        }

        /* Sorting Data and writing to a file */

//        this.temp_Name = ss[0]+(Main.counterAlias++);
//        this.fileWriter = new FileWriter("tempFolder/"+this.temp_Name+".csv");
//        Main.aliases.put(this.temp_Name,this.temp_Name);

//        ArrayList<CustomPair<String, String>> newColList = new ArrayList<>();
//        System.out.println("this: "+this.toString()+" left child name: "+this.leftChild.toString()+ "  right child: "+this.rightChild.toString());
//        ArrayList<CustomPair<String, String>> leftColumnList = Main.coltoDt.get(this.leftChild.toString());
//        ArrayList<CustomPair<String, String>> rightColumnList = Main.coltoDt.get(this.rightChild.toString());
//        for(CustomPair<String, String> temp:leftColumnList){
//            newColList.add(temp);
//        }
//        for(CustomPair<String, String> temp:rightColumnList){
//            newColList.add(temp);
//        }
//        Main.coltoDt.put(this.temp_Name,newColList);


        this.leftList = new ArrayList<>(Arrays.asList(leftExpressionColumn));
        this.rightList = new ArrayList<>(Arrays.asList(rightExpressionColumn));


//        if (this.flagForExternalSort) {
//            eR = new ExternalSort(this.leftChild.toString(), rightList, true, this.mp);
//            eS = new ExternalSort(this.rightChild.toString(), leftList, true, this.mp);
//        } else {
//            eR = new ExternalSort(this.leftChild.toString(), leftList, true, this.mp);
//            eS = new ExternalSort(this.rightChild.toString(), rightList, true, this.mp);
//        }
    }

    @Override
    public boolean hasNext() {
        if(this.firstFlag){
            if(!this.leftChild.hasNext())
                return false;
            return true;

        }
        else{
            if(this.R_S.hasNext())
                return true;
            else
                return false;
        }
    }

    @Override
    public Row next() throws SQLException, IOException {

        if(this.firstFlag) {

            this.r = this.leftChild.next();
            System.out.println("this: "+this.toString());
            this.s = this.rightChild.next();


//            ArrayList<CustomPair<String, String>> newColList = new ArrayList<>();
//            ArrayList<CustomPair<String, String>> leftColumnList = Main.coltoDt.get(this.leftChild.toString());
//            ArrayList<CustomPair<String, String>> rightColumnList = Main.coltoDt.get(this.rightChild.toString());
//
            // creating
            this.temp_Name = this.ss[0]+(Main.counterAlias++);
            this.fileWriter = new FileWriter("tempFolder/"+this.temp_Name+".csv");
            Main.aliases.put(this.temp_Name,this.temp_Name);


            this.pw = new PrintWriter(this.fileWriter);


            this.tempr = this.rightChild;
                if (this.tempr instanceof FilterIterator) {
                    while (this.tempr instanceof FilterIterator) {
//                        System.out.println("while 1");
                          this.tempr = this.tempr.leftChild;
                    }
                }

            this.templ=this.leftChild;
//                System.out.println("this is : "+ this.toString());
                if (this.templ instanceof FilterIterator) {
                    while (this.templ instanceof FilterIterator)
                        this.templ = this.leftChild.leftChild;
                }

//                System.out.println("templ: "+ this.templ.toString() + "  tempr: "+ this.tempr.toString());
                if (this.flagForExternalSort) {
//                    System.out.println("left file: "+this.templ.toString()+" is being sorted on : "+this.rightList.get(0) + " mp size is: "+ this.templ.mp.size());
                    if (this.templ instanceof ScanIterator) {
                        eR = new ExternalSort(this.templ.toString(), this.rightList, true, this.templ.mp, true);
                    } else {
                        eR = new ExternalSort(this.templ.toString(), this.rightList, true, this.templ.mp, false);
                    }
                    //                    System.out.println("right file: "+this.tempr.toString()+" is being sorted on : "+this.leftList.get(0) + " mp size is: "+ this.tempr.mp.size());

                    if (tempr instanceof ScanIterator) {
                        eS = new ExternalSort(this.tempr.toString(), this.leftList, true, this.tempr.mp, true);
                    } else {
                        eS = new ExternalSort(this.tempr.toString(), this.leftList, true, this.tempr.mp, false);
                    }
                } else {
//                    System.out.println("left file: "+this.templ.toString()+" is being sorted on : "+this.leftList.get(0) + " mp size is: "+ this.templ.mp.size());
                    if (templ instanceof ScanIterator) {
                        eR = new ExternalSort(this.templ.toString(), this.leftList, true, this.templ.mp, true);
                    } else {
                        eR = new ExternalSort(this.templ.toString(), this.leftList, true, this.templ.mp, false);
                    }
                    // System.out.println("right file: "+this.tempr.toString()+" is being sorted on : "+this.rightList.get(0)  + " mp size is: "+ this.templ.mp.size());
                    if (tempr instanceof ScanIterator) {
                        eS = new ExternalSort(this.tempr.toString(), this.rightList, true, this.tempr.mp, true);
                    } else {
                        eS = new ExternalSort(this.tempr.toString(), this.rightList, true, this.tempr.mp, false);
                    }
                }





                //System.out.println("this: "+this.toString() + "  templ: "+templ.toString()+ "  tempr :"+tempr.toString());

//                eR.sortBigFile();

//            this.r = this.leftChild.next();
//            eR.sortBigFile();
//            this.s = this.rightChild.next();

            //System.out.println("left child mp size "+this.templ.mp.size());
            eR.sortBigFile();
            eS.sortBigFile();

            if (this.leftChild instanceof ScanIterator) {
                TempFileReadIterator tempFileReadIterator = new TempFileReadIterator(this.leftChild.toString(), this.leftChild.leftChild, this.leftChild.rightChild);
                this.leftChild = tempFileReadIterator;
            }

            if (this.rightChild instanceof ScanIterator) {
                TempFileReadIterator tempFileReadIterator1 = new TempFileReadIterator(this.rightChild.toString(), this.rightChild.leftChild, this.rightChild.rightChild);
                this.rightChild = tempFileReadIterator1;
            }


            while (this.leftChild.hasNext()) {
                if(this.flagReset){
                    for(int i=0;i<mark-1;i++){
                        this.s = this.rightChild.next();
                    }
                    // List<PrimitiveValue> rTableMap;
                    // rTableMap.get(this.R.mp.get(this.leftExpressionString).getFirst())
                    //this.r!=null && this.s!=null &&
                    //this.r!=null && this.s!=null &&   this.r.RowValues!=null && this.s.RowValues!=null &&
                    while (  this.leftChild.mp!=null && this.rightChild.mp!=null && this.leftChild.hasNext()&&(this.r.RowValues.get(this.leftChild.mp.get(this.leftExpressionString).getFirst()).toString()).compareTo(this.s.RowValues.get(this.rightChild.mp.get(this.rightExpressionString).getFirst()).toString()) < 0) {

//                            System.out.println("while 2");
                            this.r = this.leftChild.next();

                    }

                    //this.r!=null && this.s!=null &&
                    while ( this.leftChild.mp!=null && this.rightChild.mp!=null &&   this.rightChild.hasNext() &&
                            (this.r.RowValues.get(this.leftChild.mp.get(this.leftExpressionString).getFirst()).toString()).compareTo(this.s.RowValues.get(this.rightChild.mp.get(this.rightExpressionString).getFirst()).toString()) > 0) {
                        this.s = this.rightChild.next();
                        this.mark++;
//                        System.out.println("while 3");
                    }
                    this.temps=this.s;
                    this.flagReset=false;
                }

//                System.out.println("this: "+ this.toString());
//                System.out.println(this.r.RowValues.get(this.leftChild.mp.get(this.leftExpressionString).getFirst()).toString());
//                System.out.println(this.s.RowValues.get(this.rightChild.mp.get(this.rightExpressionString).getFirst()).toString());
//                System.out.println("this: "+ this.toString());
//                if(this.r !=null){
//                    System.out.println("left tupple: "+ this.r.RowValues.get(this.leftChild.mp.get(this.leftExpressionString).getFirst()).toString());
//
//                }
//                if(this.s !=null){
//                    System.out.println("right tupple: "+ this.s.RowValues.get(this.rightChild.mp.get(this.rightExpressionString).getFirst()).toString());
//                }


                //this.s!=null &&
                if (this.leftChild.mp!=null && this.rightChild.mp!=null &&(this.r.RowValues.get(this.leftChild.mp.get(this.leftExpressionString).getFirst()).toString()).equals(this.s.RowValues.get(this.rightChild.mp.get(this.rightExpressionString).getFirst()).toString())) {
                    this.result = merge(this.r.RowValues, this.s.RowValues);
                    this.s = this.rightChild.next();
                    if(this.s == null){
                        this.rightChild.reset();
                        this.s=this.rightChild.next();
                        this.r = this.leftChild.next();
                        this.flagReset=true;
                    }
                } else {
                    //need to hndle if right child i  filter iterator
                    this.rightChild.reset();
                    this.s=this.rightChild.next();
                    this.r = this.leftChild.next();
                    this.flagReset=true;
                    this.result=null;
                }
                writeTofile();
                //this.result=null;
                //System.out.println("------------");

            }




            // for last tupple of R
            if(this.leftChild!=null){
                this.rightChild.reset();
                this.s=this.rightChild.next();
//                for(int i=0;i<this.mark-1;i++)
//                    this.s = this.rightChild.next();
                if(this.s!=null) {
                    while (this.s != null) {
//                        System.out.println("while 4");
//                        if (this.toString().equals("SUPPLIER1")) {
//                            System.out.println("this is : " + this.toString());
//                            System.out.println("left value being comapred is:" + this.r.RowValues.get(this.leftChild.mp.get(this.leftExpressionString).getFirst()).toString());
//                            System.out.println("right value being comapred is:" + this.s.RowValues.get(this.rightChild.mp.get(this.rightExpressionString).getFirst()).toString());
//                        }

                        //this.r != null && this.s != null &&
//                        System.out.println("this: "+ this.toString());
//                        if(this.r !=null){
//                            System.out.println("left tupple: "+ this.r.RowValues.get(this.leftChild.mp.get(this.leftExpressionString).getFirst()).toString());
//
//                        }
//                        if(this.s !=null){
//                            System.out.println("right tupple: "+ this.s.RowValues.get(this.rightChild.mp.get(this.rightExpressionString).getFirst()).toString());
//                        }
                        if (this.leftChild.mp!=null && this.rightChild.mp!=null  && (this.r.RowValues.get(this.leftChild.mp.get(this.leftExpressionString).getFirst()).toString()).equals(this.s.RowValues.get(this.rightChild.mp.get(this.rightExpressionString).getFirst()).toString())) {
                            this.result = merge(r.RowValues, s.RowValues);
                            this.s = this.rightChild.next();
                            writeTofile();
                            this.result = null;
                        }
                        else{
                            this.s = this.rightChild.next();
                            this.result = null;
                        }
//                    else{
//                        break;
//                    }
                    }
                }

                this.r=this.leftChild.next();
            }

//            fileOutputStream.flush();
//            fileOutputStream.close();

            this.firstFlag=false;
            try {
                Main.aliases.put(this.temp_Name,this.temp_Name);
                this.R_S = new TempFileReadIterator(this.temp_Name, this.leftChild, this.rightChild);
            } catch (IOException e) {
                e.printStackTrace();
            }

            // prone to hve problem
            ArrayList<CustomPair<String, String>> newColList = new ArrayList<>();
            ArrayList<CustomPair<String, String>> leftColumnList = Main.coltoDt.get(this.leftChild.toString());
            ArrayList<CustomPair<String, String>> rightColumnList = Main.coltoDt.get(this.rightChild.toString());
            //System.out.println(this.templ.toString());
            for(CustomPair<String, String> temp:leftColumnList){
                newColList.add(temp);
            }
//            System.out.println(this.tempr.toString());
            for(CustomPair<String, String> temp:rightColumnList){
                newColList.add(temp);
            }
            Main.coltoDt.put(this.temp_Name,newColList);
//            System.out.println("this: "+this.toString()+"  this mp size"+newColList.size()+"  left child: "+this.leftChild.toString()+"  left child mp size "+ this.leftChild.mp.size()+"   right child "+this.rightChild.toString()+"  right child mp size "+ this.rightChild.mp.size());
            //System.out.println("in Sort merger join file nme : "+ this.temp_Name + " file's coltodt list: "+newColList.size());
            this.pw.flush();
            this.pw.close();
        }
        return this.R_S.next();

    }


    public void writeTofile(){
//        System.out.println("this: "+this.toString() + "  filname: "+ this.temp_Name);
//        System.out.println("tuple written: "+ (++this.cntt));
        if (this.result!=null) {
//            System.out.println("this: "+this.toString() + "  filname: "+ this.temp_Name);
//            System.out.println("tuple written: "+ (++this.cntt));
            int count=0;
            for(PrimitiveValue p: this.result.RowValues) {
                count++;
                try {
                    this.pw.print(p.toRawString());
                    if (count != this.result.RowValues.size()) {
                        this.pw.print("|");
                    }
                    else {
                        this.pw.print("\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }

    }


    @Override
    public void reset() throws IOException {
        this.R_S.reset();
    }


    public Row merge(List<PrimitiveValue> r1, List<PrimitiveValue> r2) {
//        System.out.println("merged");
        Row newRow = new Row();
        newRow.RowValues.addAll(r1);
        newRow.RowValues.addAll(r2);
        return newRow;
    }

    public String toString(){
        return this.temp_Name;
    }

}

