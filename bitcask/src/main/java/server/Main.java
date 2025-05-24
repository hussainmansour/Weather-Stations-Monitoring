package server;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class Main {
    static Bitcask bitcask = Bitcask.getBitcaskInstance();

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        long x = 0;
        while (System.currentTimeMillis() - start < 900_000) {
            for (int i = 0; i < 10; i++) {
                add(x++);
            }
        }
        for (int i = 0; i < 3; i++) {
            byte[] a = bitcask.get(i).getValue();
            System.out.println(ByteBuffer.wrap(a).getLong());
        }
        System.out.println("x = " + x);
    }

    public static void add(long i) {
        DataEntry entry = new DataEntry();
        byte[] a = ByteBuffer.allocate(Long.BYTES).putLong(i).array();
        ArrayList<Byte> aa = new ArrayList<>();
        for (byte b : a)
            aa.add(b);
        entry.createEntry(i % 3, aa);
        bitcask.add(entry);
    }
}