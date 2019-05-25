package dubstep.Optimizer;

import dubstep.CustomClasses.CustomPair;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Pattern;

public class TreeMapIndex implements Serializable {

    private final int FILESIZE = 100000;
    private FileReader fileReader;
    private BufferedReader bufferedReader;
    private RandomAccessFile randomAccessFile;
    private List<Integer> position;
    private List<String> indexColumnName;
    private String fileName;
    Pattern pattern;
    Pattern patternDot;
    List<String> list = new ArrayList<>();
    private FileOutputStream fileOutputStream;
    private ObjectOutputStream objectOutputStream;
    private HashMap<String, TreeMap<String, List<CustomPair<Integer, String>>>> mp;
    int filecounter;

    private TreeMapIndex(String fileName, List<String> indexColumnName, List<Integer> position) {
        this.mp = new HashMap<>();
        this.fileName = fileName;
        this.indexColumnName = indexColumnName;
        this.position = position;
        this.pattern = Pattern.compile("\\|");
        this.patternDot = Pattern.compile("\\.");
        this.filecounter = 0;

        try {
            this.randomAccessFile = new RandomAccessFile("/Users/shubham/DB/data/"+this.fileName + ".csv", "rw");
            this.fileReader = new FileReader("/Users/shubham/DB/data/"+fileName + ".csv");
            this.bufferedReader = new BufferedReader(this.fileReader);
            //this.fileOutputStream = new FileOutputStream("tempFolder/"+this.fileName+"-"+this.indexColumnName+".csv");
            //this.objectOutputStream = new ObjectOutputStream(fileOutputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createIndex() {
        String line;
        String value = "";
        long len = 0;
        int local_counter = 0;

        try {
            while ((line = this.bufferedReader.readLine()) != null) {

//                value = getValue(line, this.position);
//                list.add(value);
//                len+=line.getBytes().length+1;


                for (int i=0;i<this.position.size();i++) {
                    value = getValue(line, this.position.get(i));
                    if (mp.containsKey(indexColumnName.get(i))) {
                        TreeMap<String, List<CustomPair<Integer, String>>> treeMap = mp.get(indexColumnName.get(i));
                    } else {
                        TreeMap<String, List<CustomPair<Integer, String>>> treeMap = new TreeMap<>();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getValue(String line, int position) {
        String[] parts = this.pattern.split(line);
        return parts[position];
    }
}
