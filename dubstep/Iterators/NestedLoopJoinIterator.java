package dubstep.Iterators;

import dubstep.CustomClasses.CustomPair;
import dubstep.Row.Row;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**Author : Shubham Gulati **/


public class NestedLoopJoinIterator extends Iterator {

    Row currentR;
    Iterator R;
    Iterator S;

    @Override
    public boolean hasNext() {
        if (!this.leftChild.hasNext() && !this.rightChild.hasNext()) {
            return false;
        }
        return true;
    }

    public NestedLoopJoinIterator(Iterator R, Iterator S) {
        this.R = R;
        this.S = S;
        this.leftChild = R;
        this.rightChild = S;
        this.iteratorLeftList = new ArrayList<>();
        this.iteratorRightList = new ArrayList<>();

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

        if (this.leftChild.iteratorLeftList != null) {
            String listString = String.join(", ", this.leftChild.iteratorLeftList);
//            System.out.println("left child left list is " + listString);
            this.iteratorLeftList.addAll(this.leftChild.iteratorLeftList);
        }

        if (this.leftChild.iteratorRightList != null) {
            String listString = String.join(", ", this.leftChild.iteratorRightList);
//            System.out.println("left child right list is " + listString);
            this.iteratorLeftList.addAll(this.leftChild.iteratorRightList);
        }

        if (this.rightChild.iteratorLeftList != null) {
            String listString = String.join(", ", this.rightChild.iteratorLeftList);
//            System.out.println("right child left list is " + listString);
            this.iteratorRightList.addAll(this.rightChild.iteratorLeftList);
        }

        if (this.rightChild.iteratorRightList != null) {
            String listString = String.join(", ", this.rightChild.iteratorRightList);
//            System.out.println("right child right list is " + listString);
            this.iteratorRightList.addAll(this.rightChild.iteratorRightList);
        }
    }

    @Override
    public Row next() throws IOException, SQLException {
        if (!this.S.hasNext()) {
            this.S.reset();
            this.currentR = null;
        }

        if (this.currentR == null && this.R.hasNext()) {
            this.currentR = this.R.next();
        }

        return merge(this.currentR, this.S.next());
    }

    public Row merge(Row r1, Row r2) {
        Row newRow = new Row();
        newRow.RowValues.addAll(r1.RowValues);
        newRow.RowValues.addAll(r2.RowValues);
        return newRow;
    }

    @Override
    public void reset() {

    }


    public String toString(){
        String temp = "The two tables of NLJ are: "+R+" " +S;
        return temp;
    }

    @Override
    public String getName() {
        return "The two tables of NLJ are: "+R+" " +S;
    }
}
