package dubstep.Iterators;

import dubstep.Row.Row;
import dubstep.Main;
import dubstep.Row.Row;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IndexNestedLoopJoin extends Iterator {

    boolean setFlag=true;
    String colVal="";
    private Row newRow=null;
    private int lenghthTuple=0;
    private int indexTuple=0;
    Row S;
    Row result;
    List<Row> tuples=null;
    private String rightExpressionString = null;
    private Expression rightExpression = null;
    RandomAccessFile fileStore;
    String tableName;
    boolean firstFlagforInitializing = true;
    String rightTableName;


    //assumption that my index is on left table
    //if i need to change this i will have to handle it
    public IndexNestedLoopJoin(Iterator R, Iterator S, Expression expression) throws IOException{

        this.rightExpression = ((BinaryExpression) expression).getRightExpression();
        Column rightExpressionColumn = (Column) rightExpression;
        this.rightTableName = rightExpressionColumn.getTable().getName();
        this.rightExpressionString = this.rightTableName + "." + rightExpressionColumn.getColumnName();
        this.leftChild=R;
        this.rightChild=S;
    }

    @Override
    public boolean hasNext() {
        if (this.rightChild.hasNext() || this.indexTuple > 0)
            return true;
        return false;
    }

    @Override
    public Row next() throws SQLException, IOException {

        //this is for initialising random access file on which the index is there
        //this will happen onlyh one time
        if(this.firstFlagforInitializing){
            try {
                this.fileStore = new RandomAccessFile("/home/vallabh/UB/Database/data/"+"LINEITEM"+".csv", "rw");//-------------need to find a way to get file name
            } catch (FileNotFoundException e){
                this.fileStore=null;
                e.printStackTrace();
            }
            this.firstFlagforInitializing=false;
        }

        if(this.rightChild.hasNext()){
            if(this.setFlag) {
                System.out.println("next child of right ");
                System.out.println("right child is: "+this.rightChild.toString());
                this.S = this.rightChild.next();
                if(this.S!=null) {
                    this.tuples = this.getTuples(this.rightExpressionString, this.S);
                    if (this.tuples != null) {
                        this.lenghthTuple = this.tuples.size();
                        this.indexTuple = this.lenghthTuple;
                        if (this.indexTuple > 1) {
                            this.setFlag = false;
                        }
                    }
                }
            } else {
                this.indexTuple=0;
            }

            if(this.indexTuple>0){

                this.result = merge(this.tuples.get(this.lenghthTuple-this.indexTuple).RowValues, this.S.RowValues);
                //System.out.println(Arrays.toString(this.result.RowValues.toArray()));
                System.out.println("merged");
                this.indexTuple--;
                if(this.indexTuple<=0){
                    System.out.println("now will got to next tuple of right child: ");
                    this.setFlag=true;
                }
            } else {// setting object references to null
                //maybe call a function to go back
                System.out.println("now will got to next tuple of right child: ");
                this.setFlag=true;
                this.result = null;
                this.tuples=null;
            }

        }

        //when on last tuple of right table and there are multiple rows to be joined
        if(!this.rightChild.hasNext()  && this.indexTuple>0){
            this.result = merge(this.tuples.get(this.lenghthTuple-this.indexTuple).RowValues, this.S.RowValues);
            this.indexTuple--;
        }

//        System.out.println(Arrays.toString(this.result.RowValues.toArray()));
        return this.result;
    }

    private List<Row> getTuples(String ExpressionString, Row rS) {

        System.out.println(rS.RowValues.size());

        List<PrimitiveValue> sTableMap = rS.RowValues;
        this.colVal = String.valueOf(sTableMap.get(this.rightChild.mp.get(ExpressionString).getFirst()));
        System.out.println("this.colvalue: "+this.colVal);

        //search this col value in the indexMap
        List<Long> temp = null;

        //**************************
        //temp =Main.hmapIndex.get(this.colVal);

        //iterate over list of long bytes and get the row from the file and add it to the list
        List<Row> resRow=null;
        if(temp!=null) {
            for (int i = 0; i < temp.size(); i++) {
                try {
                    this.fileStore.seek(temp.get(i));
                } catch (IOException e) {
                    this.fileStore = null;
                    e.printStackTrace();
                }
                try {
                    resRow = new ArrayList<>();
                    String templine = this.fileStore.readLine();
                    System.out.println(templine);

                //*****************************
                    //resRow.add(Main.convertToRows(templine, "LINEITEM"));


                } catch (IOException e) {
                    resRow = null;
                    e.printStackTrace();
                }
            }
        }
//        System.out.println("get tuples: "+resRow.size());
        return resRow;
    }


    public Row merge (List < PrimitiveValue > r1, List < PrimitiveValue > r2){
//        System.out.println("merged");
        this.newRow = new Row();
        this.newRow.RowValues.addAll(r1);
        this.newRow.RowValues.addAll(r2);
        return this.newRow;
    }

    @Override
    public void reset() throws IOException {

    }
}
