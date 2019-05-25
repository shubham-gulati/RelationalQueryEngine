package dubstep.Optimizer;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ProjectionPushdownWrite {


    /**
     * This class writes all columns of the table in Seperate Files
     *
     *
     */

    private String tableName;
    private FileReader fileReader;
    private BufferedReader bufferedReader;
    private List<ColumnDefinition> columnDefinitions;
    private List<FileWriter> fileWriters;
    //private List<BufferedWriter> bufferedWriters;
    private List<PrintWriter> printWriters;


    public ProjectionPushdownWrite(String tableName, List<ColumnDefinition> columnDefinitionList) {
        String filePath = "projections";
        Path path = Paths.get(filePath);

        try {
            if (!Files.exists(path)) {
                Files.createDirectory(path);
            }
            //this.fileReader = new FileReader("/Users/shubham/DB/data/"+tableName+".csv");
//            this.fileReader = new FileReader("/Users/adityaagarwal/Documents/Spring 2019/DBMS/Queries/Sanity_Check_Examples/data/" + tableName + ".csv");
            this.fileReader = new FileReader("data/" + tableName + ".csv");
            ///home/vallabh/UB/Database/data/
//            this.fileReader = new FileReader("/home/vallabh/UB/Database/data/" + tableName + ".csv");///home/vallabh/UB/Database/data/

            this.bufferedReader = new BufferedReader(this.fileReader);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.tableName = tableName;
        this.columnDefinitions = columnDefinitionList;
        initializeWriters(this.tableName, this.columnDefinitions);
    }

    public void initializeWriters(String tableName, List<ColumnDefinition> list) {

        try {
            this.fileWriters = new ArrayList<>();
            //this.bufferedWriters = new ArrayList<>();
            this.printWriters = new ArrayList<>();


            for (int i = 0; i < list.size(); i++) {
                String colName = list.get(i).getColumnName();
                FileWriter fileWriter = new FileWriter("projections/" + tableName + "-" + colName + ".txt");
                this.fileWriters.add(fileWriter);
                //BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
                PrintWriter printWriter = new PrintWriter(fileWriter);
                //this.bufferedWriters.add(bufferedWriter);
                this.printWriters.add(printWriter);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeToDisk() {
        try {
            String line;
            while  ((line = this.bufferedReader.readLine()) != null) {
                String[] parts = line.split("\\|");

                for (int i=0; i<parts.length; i++) {
                    this.printWriters.get(i).println(parts[i]);
                    //this.bufferedWriters.get(i).write(parts[i]);
                }
            }

            closeAll();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeAll() {
        try {
            for (int i = 0; i < this.columnDefinitions.size(); i++) {
                this.fileWriters.get(i).close();
                this.printWriters.get(i).flush();
                this.printWriters.get(i).close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
