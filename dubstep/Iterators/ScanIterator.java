package dubstep.Iterators;

import dubstep.CustomClasses.CustomPair;
import dubstep.Main;
import dubstep.Row.Row;
import net.sf.jsqlparser.expression.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**Author : Shubham Gulati **/

public class ScanIterator extends Iterator {

    public List<BufferedReader> bufferedReaderList;
    public List<FileReader> fileReaderList;
    private String[] tempArray;
    String tableName;
    String alias;
    Pattern pattern;
    Pattern patternSpace;
    List<String> lstColDataType;
    public ArrayList<Row> rowList = new ArrayList<>();
    public HashMap<String,Integer> coltoIndex;
    public ArrayList<String> colList;
    boolean fileEmpty=false;


    public ScanIterator(String tableName, String alias,ArrayList<String> colList,ArrayList<Row> rowList) throws IOException {
        this.colList=colList;
        this.coltoIndex = new HashMap<>();
        this.rowList = rowList;
        this.fileReaderList = new ArrayList<>();

        int j=0;
        ArrayList<CustomPair<String,String>> temp = Main.coltoDt.get(tableName);
        for(CustomPair<String,String> cs: temp){
            coltoIndex.put(cs.getFirst(),j++);
        }

        for (int i=0;i<colList.size();i++) {
//            FileReader fileReader = new FileReader("/Users/adityaagarwal/Documents/Spring 2019/DBMS/Project/team17/projections/"+tableName+"-"+colList.get(i)+".txt");
            FileReader fileReader = new FileReader("projections/"+tableName+"-"+colList.get(i)+".txt");
            this.fileReaderList.add(fileReader);
        }

        this.bufferedReaderList = new ArrayList<>();
        for (int i=0;i<colList.size();i++) {
            BufferedReader bufferedReader = new BufferedReader(this.fileReaderList.get(i), 10000000);
            this.bufferedReaderList.add(bufferedReader);
        }

        this.tempArray = new String[colList.size()];

        this.mp = new HashMap<>();
        this.alias = alias;
        this.tableName = Main.aliases.get(alias);
        this.iteratorLeftList = new ArrayList<>();
        this.iteratorLeftList.add(tableName);
        ArrayList<CustomPair<String, String>> stringList = Main.coltoDt.get(tableName);
        this.lstColDataType = new ArrayList<>();
        HashMap<String,String> mp6 = new HashMap<>();

        for (int i=0; i<stringList.size(); i++) {
            mp6.put(stringList.get(i).getFirst(),stringList.get(i).getSecond());
        }

        for(int i=0;i<colList.size();i++){
            String colName = colList.get(i);
            lstColDataType.add(mp6.get(colName));
            CustomPair<Integer, String> cr = new CustomPair(i, mp6.get(colName));
            mp.put(this.alias+"."+colName,cr);
        }

        mp6=null;

        this.iteratorRightList = null;
        pattern = Pattern.compile("\\|");
        patternSpace = Pattern.compile("\\ ");
    }

    @Override
    public boolean hasNext() {

        String line;
        try {
            if(!this.fileEmpty) {
                Main.csvTuple =true;
                for (int i = 0; i < this.bufferedReaderList.size(); i++) {
                    line = this.bufferedReaderList.get(i).readLine();

                    if (line == null) {
                        fileEmpty = true;
                        break;
                    } else {
                        this.tempArray[i] = line;
                    }
                }
            }
            if(this.fileEmpty){
                Main.csvTuple =false;
                if(this.rowList==null||this.rowList.size()==0){
                    return false;
                }
                else {
                    Row row = this.rowList.remove(0);
                    for (int i = 0; i < this.colList.size(); i++) {
                        this.tempArray[i] = row.RowValues.get(this.coltoIndex.get(colList.get(i))).toRawString();
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
            return true;
    }


    @Override
    public Row next() {

            List<PrimitiveValue> data = new ArrayList<>();

            for(int i=0;i<tempArray.length;i++){
                String type = lstColDataType.get(i).toLowerCase();

                if (type.equals("int")) {
                    Long l = Long.parseLong(tempArray[i]);
                    LongValue l1 = new LongValue(l);
                    data.add(l1);
                } else if (type.equals("string") || type.equals("varchar") || type.equals("char")) {
                    StringValue s1 = new StringValue(tempArray[i]);
                    data.add(s1);
                } else if (type.equals("decimal")) {
                    Double d = Double.parseDouble(tempArray[i]);
                    DoubleValue d1 = new DoubleValue(d);
                    data.add(d1);
                } else if (type.equals("date")) {
                    DateValue dateValue = new DateValue(tempArray[i]);
                    data.add(dateValue);
                }
            }

            Row r = new Row();
            r.RowValues = data;
            return r;
    }


    @Override
    public void reset() {
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public String getName() {
        return tableName;
    }

    public String toString(){
        return this.tableName;
    }

}
