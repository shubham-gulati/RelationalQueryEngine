package dubstep.Optimizer;

import dubstep.Row.Row;

import java.io.*;

public class OutputStreamMegalodons {

    FileOutputStream fileOutputStream;
    FileInputStream fileInputStream;
    ObjectOutputStream objectOutputStream;
    ObjectInputStream objectInputStream;

    public OutputStreamMegalodons(String tableName, boolean write) throws IOException {
        if (write) {
            fileOutputStream = new FileOutputStream("/Users/shubham/DB/data/"+tableName+".txt");
            objectOutputStream = new ObjectOutputStream(fileOutputStream);
        } else {
            fileInputStream = new FileInputStream("/Users/shubham/DB/data/"+tableName+".txt");
            objectInputStream = new ObjectInputStream(fileInputStream);
        }
    }

    public void write(Row row) throws IOException {
        objectOutputStream.writeObject(row);
    }

    public Row read() throws IOException, ClassNotFoundException {
        try {
            Row row = (Row) objectInputStream.readObject();
            return row;
        } catch (EOFException e) {
            return null;
        }
    }

    public void closeOutputStream() throws IOException {
        objectOutputStream.close();
    }

    public void closeInputStream() throws IOException {
        objectInputStream.close();
    }
}
