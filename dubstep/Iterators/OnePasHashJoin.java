package dubstep.Iterators;

import dubstep.CustomClasses.CustomPair;
import dubstep.Iterators.Iterator;
import dubstep.Main;
import dubstep.Row.Row;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Array;
import java.sql.SQLException;
import java.util.*;

public class OnePasHashJoin extends Iterator {
    private Expression leftExpression = null;
    private String leftExpressionString = null;
    private String rightExpressionString = null;
    private Expression rightExpression = null;
    HashMap<String, List<List<PrimitiveValue>>> mp3;// = new HashMap<>();// map of col values of left table and tuples as values
    Iterator R;
    Iterator S;
    private String leftTableName = null;
    private String rightTableName = null;
    List<Row> mergeList;// = new ArrayList<Row>();
    boolean firstFlag2;//=true;
    Row result;
    int cor;//=-1;
    Row rtemp = null;
    Row newRow;

    public OnePasHashJoin(Iterator R, Iterator S, Expression expression) throws IOException {

        cor = -1;
        this.result = null;
        Main.crr++;
        this.mp3 = new HashMap<>(200000);
        this.firstFlag2 = true;
        this.mergeList = new ArrayList<Row>();
        this.cor = Main.crr;
        Main.rrr.add(this.cor, new ArrayList<>());
        this.R = R;
        this.S = S;
        this.leftChild = R;
        this.rightChild = S;
        this.leftExpression = ((BinaryExpression) expression).getLeftExpression();
        this.rightExpression = ((BinaryExpression) expression).getRightExpression();


        if ((this.leftChild.mp != null) && (this.rightChild.mp != null)) {
            this.mp = new HashMap<>();
            int size1 = this.leftChild.mp.size();
            this.mp.putAll(this.leftChild.mp);
            Map<String, CustomPair<Integer, String>> temp = new HashMap<>();

            for (Map.Entry<String, CustomPair<Integer, String>> entry : this.rightChild.mp.entrySet()) {
                CustomPair<Integer, String> val = entry.getValue();
                CustomPair<Integer, String> newVal = new CustomPair<>(val.getFirst() + size1, val.getSecond());
                temp.put(entry.getKey(), newVal);
            }

            this.mp.putAll(temp);
        }

        //identify here whether expression is in for R.A or A
        Column leftExpressionColumn = (Column) leftExpression;
        Column rightExpressionColumn = (Column) rightExpression;
        this.leftTableName = leftExpressionColumn.getTable().getName();

        if (this.leftTableName == null || this.leftTableName.equals(null)) {
            this.leftTableName = Main.CtoT.get(leftExpressionColumn.toString());
            this.leftExpressionString = leftTableName + "." + leftExpressionColumn.getColumnName();
        } else {
            this.leftExpressionString = this.leftExpression.toString();
        }

        this.rightTableName = rightExpressionColumn.getTable().getName();
        if (this.rightTableName == null || this.rightTableName.equals(null)) {
            this.rightTableName = Main.CtoT.get(rightExpressionColumn.toString());
            this.rightExpressionString = this.rightTableName + "." + rightExpressionColumn.getColumnName();
        } else {
            this.rightExpressionString = this.rightExpression.toString();
        }

        //to correct left right order of coming expression
        String tempExpression = "";

        String[] ss = this.rightChild.toString().split("\\.");


        if(ss[0].startsWith("NOT")){
            ss[0]=ss[0].substring(ss[0].indexOf("(")+1);
        }



        if (!(ss[0].equals(this.rightTableName))  ) {
                tempExpression = this.leftExpressionString;
                this.leftExpressionString = this.rightExpressionString;
                this.rightExpressionString = tempExpression;
        }
    }

    public void createHash(Iterator R) throws IOException, SQLException {
        List<PrimitiveValue> rTableMap;

        while (this.leftChild.hasNext()) {
            //System.out.println("hash of  "+this.leftChild.toString());
            Row tempr = this.leftChild.next();
            if (tempr != null) {
                rTableMap = tempr.RowValues;
                String colTobeHashed = String.valueOf(rTableMap.get(this.leftChild.mp.get(this.leftExpressionString).getFirst())); //get string value of the col of the current tuple
                String hashtext = colTobeHashed;// get the hash value

                if (this.mp3.containsKey(hashtext)) {
                    List<List<PrimitiveValue>> mnc = this.mp3.get(hashtext);
                    mnc.add(rTableMap);
                    this.mp3.put(hashtext, mnc);
                    mnc=null;
                } else {
                    List<List<PrimitiveValue>> temp = new ArrayList<>();
                    temp.add(rTableMap);
                    this.mp3.put(hashtext, temp);
                    temp=null;
                }
            }
        }
    }


    @Override
    public boolean hasNext() {
        if(firstFlag2){
        if (this.rightChild.hasNext()) {
            this.R = this.leftChild;
            this.S = this.rightChild;

        }
            return true;
        } else {
            if (this.lennn > 0 || this.rightChild.hasNext())
                return true;
            else {
                this.mp3=null;
                return false;
            }
        }
    }

    boolean flag111 = false;
    Row rr12 = null;
    String colTobeHashedSSS = "";
    int lennn = -1;
    String hashtext123 = "";

    public void function1() throws IOException, SQLException {
        if (!this.flag111) {
                this.rr12 = this.rightChild.next();
                if (this.rr12 != null) {
                    List<PrimitiveValue> sTableMap = this.rr12.RowValues;
                    this.colTobeHashedSSS = String.valueOf(sTableMap.get(this.rightChild.mp.get(this.rightExpressionString).getFirst()));
                    hashtext123 = this.colTobeHashedSSS;
                    if (this.mp3.containsKey(this.hashtext123)) {
                        this.flag111 = true;
                        this.lennn = this.mp3.get(this.hashtext123).size();
                    } else {
                        this.flag111 = false;
                        this.rr12=null;
                    }
                }

        }
    }

    public void function2() {

        if (this.rr12 != null) {
            List<PrimitiveValue> sTableMap = this.rr12.RowValues;
            List<List<PrimitiveValue>> rTableList = this.mp3.get(this.hashtext123);
            if (rTableList != null && rTableList.size() > 0) {
                List<PrimitiveValue> tempMap = rTableList.get(rTableList.size() - this.lennn);
                if (tempMap != null && sTableMap != null) {
                    if ((sTableMap.get(this.rightChild.mp.get(this.rightExpressionString).getFirst()).toString()).equals(tempMap.get(this.leftChild.mp.get(this.leftExpressionString).getFirst()).toString())) {//genereally null pointer of expression on either side
                        this.rtemp = merge(tempMap, sTableMap);
                        this.newRow=null;
                        this.lennn--;


                        if (this.lennn <= 0)
                            this.flag111 = false;
                    } else {

                        this.lennn--;
                        this.rtemp = null;
                        if (this.lennn <= 0) {
                            this.flag111 = false;
                        } else {
                        }
                    }
                }
                else{
                    this.rtemp=null;
                }
            } else{
                this.rtemp=null;
            }
        }
        else{
            this.rtemp=null;
        }
    }

    @Override
    public Row next() throws SQLException, IOException {
            Iterator TEMPChild;
            if (this.firstFlag2) {

                this.R = this.leftChild;
                this.S = this.rightChild;
                try {
                    createHash(this.leftChild);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                this.firstFlag2 = false;
            }

            this.function1();
            this.function2();

            return this.rtemp;
        }


        public Row merge (List < PrimitiveValue > r1, List < PrimitiveValue > r2){
            this.newRow = new Row();
            this.newRow.RowValues.addAll(r1);
            this.newRow.RowValues.addAll(r2);
            return this.newRow;
        }


        public String toString () {
            return this.leftChild.toString() + " " + this.rightChild.toString();
        }

        public String getExpression () {
            return this.leftExpressionString + " " + this.rightExpressionString;
        }

        @Override
        public void reset () {

        }
    }

