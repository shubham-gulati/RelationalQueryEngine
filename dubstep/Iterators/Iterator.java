package dubstep.Iterators;

import dubstep.CustomClasses.CustomPair;
import dubstep.Row.Row;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

public abstract class Iterator {

    public String name;
    public Iterator leftChild;
    public Iterator rightChild;
    public List<String> iteratorLeftList;
    public List<String> iteratorRightList;
    public HashMap<String, CustomPair<Integer, String>> mp;

    public abstract boolean hasNext();

    public abstract Row next() throws SQLException, IOException;

    public abstract void reset() throws IOException;

    public Iterator() {

    }

    public String getName() {
        return name;
    }

    public Iterator getLeftChild() {
        return leftChild;
    }

    public Iterator getRightChild() {
        return rightChild;
    }
}
