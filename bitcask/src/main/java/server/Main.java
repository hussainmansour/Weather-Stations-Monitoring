package server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Main {
    static Bitcask bitcask = Bitcask.getBitcaskInstance();

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        int x = 0;
        while (System.currentTimeMillis() - start < 900_000) {
            for (int i = 0; i < 10; i++) {
                add(x++);
            }
        }
        for (int i = 0; i < 3; i++) {
            System.out.println(bitcask.get(i).getValue()[1]);
        }

    }

    public static void add(int i) {
        DataEntry entry = new DataEntry();
        ArrayList<Byte> a = new ArrayList<>();
        a.add((byte) 1);
        a.add((byte) i);
        entry.createEntry(i % 3, a);
        bitcask.add(entry);
    }
}