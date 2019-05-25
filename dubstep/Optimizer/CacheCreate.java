package dubstep.Optimizer;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CacheCreate {

    public static void cacheToDisk(String tableName, String write) throws IOException {
        String filePath = "createCommands";
        Path path = Paths.get(filePath);

        if (!Files.exists(path)) {
            Files.createDirectory(path);
        }

        FileWriter fw = new FileWriter(filePath+"/"+tableName+".csv");
        PrintWriter pw = new PrintWriter(fw);
        pw.println(write);
        pw.flush();
        pw.close();
        fw.close();
    }
}
