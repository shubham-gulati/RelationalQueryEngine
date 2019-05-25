package dubstep.CustomClasses;

/**Author : Shubham Gulati **/

public class CustomPair<U, V> {

    public U first;
    public V second;


    public CustomPair(U first, V second) {
        this.first = first;
        this.second = second;
    }

    public U getFirst() {
        return first;
    }

    public V getSecond() {
        return second;
    }
}
