package dubstep.Iterators;

import dubstep.CustomClasses.CustomPair;
import dubstep.Main;
import dubstep.Row.Row;
import net.sf.jsqlparser.expression.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TempFileReadIterator extends Iterator {

    private BufferedReader bufferedReader;
    private FileReader fileReader;
    private ArrayList<String> arrayList;
    private final int SIZE = 100;
    private String fileName;
    private String filePath;


    public TempFileReadIterator(String filename, Iterator iterator1, Iterator iterator2) throws IOException {
        this.fileName = filename;
        System.out.println("tempreader "+this.fileName);
        this.filePath = "tempFolder/"+this.fileName+".csv";
        this.arrayList = new ArrayList<>();

        this.leftChild = iterator1;
        this.rightChild = iterator2;

        this.fileReader = new FileReader(this.filePath);
        this.bufferedReader = new BufferedReader(this.fileReader);

        if (this.leftChild != null && this.rightChild != null) {
            if ((this.leftChild.mp != null) && (this.rightChild.mp != null)) {
                this.mp = new HashMap<>();
                int size1 = this.leftChild.mp.size();
                int size2 = this.rightChild.mp.size();
                this.mp.putAll(this.leftChild.mp);
                Map<String, CustomPair<Integer, String>> temp = new HashMap<>();

                for (Map.Entry<String, CustomPair<Integer, String>> entry : this.rightChild.mp.entrySet()) {
                    CustomPair<Integer, String> val = entry.getValue();
                    CustomPair<Integer, String> newVal = new CustomPair<>(val.getFirst() + size1, val.getSecond());
                    temp.put(entry.getKey(), newVal);
                }
                this.mp.putAll(temp);
            }
        }
    }

    public void readTuples(String line1) throws IOException {
        String line;
        arrayList.add(line1);
        while (this.arrayList.size() < SIZE && (line = this.bufferedReader.readLine()) != null) {
            arrayList.add(line);
        }
    }

    @Override
    public boolean hasNext() {
        String line;
        try {
            if (this.arrayList.size() == 0 && (line = this.bufferedReader.readLine()) != null) {
                readTuples(line);
                return true;
            } else if (this.arrayList.size() > 0) {
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public Row next() {

        if (this.hasNext()) {
            String[] parts = this.arrayList.get(0).split("\\|");

            arrayList.remove(0);
            List<PrimitiveValue> data = new ArrayList<>();

            ArrayList<CustomPair<String, String>> coltoDataList = Main.coltoDt.get(this.fileName);
//            System.out.println("filename is "+this.fileName+"  this file's coldtotodt "+coltoDataList.size() + "  columns/parts : "+parts.length);
            //            System.out.println();
//            System.out.println();

            for (int i = 0; i < parts.length; i++) {
//                System.out.println("fileName: "+this.fileName+"  size of colto  dt: "+coltoDataList.size()+"  parts length: "+ parts.length);
                String type = coltoDataList.get(i).getSecond().toLowerCase();
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
        return null;
    }

    @Override
    public void reset() throws IOException {
        this.fileReader.close();
        this.bufferedReader.close();
        this.fileReader = new FileReader(this.filePath);
        this.bufferedReader = new BufferedReader(this.fileReader);
        this.arrayList.clear();
    }

    public void resetforTemp(String fileName) throws IOException {
        this.fileReader.close();
        this.bufferedReader.close();
        this.fileReader = new FileReader("tempFolder/"+fileName+".csv");
        this.bufferedReader = new BufferedReader(this.fileReader);
        this.arrayList.clear();
    }

    public String toString(){
        return this.fileName;
    }
}
