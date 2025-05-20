package server;

import java.util.ArrayList;

public class Main {
    public static void main(String[] args) {
        FileManager fileManager = new FileManager();
        fileManager.createDir();
        fileManager.createNewFile();
        Entry entry = new Entry();
        ArrayList<Byte> a = new ArrayList<>();
        a.add((byte) 1);
        a.add((byte) 2);
        a.add((byte) 3);
        a.add((byte) 4);
        a.add((byte) 5);
        a.add((byte) 6);
        entry.createEntry(0, a);
        fileManager.append(entry);
        System.out.println(fileManager.getActiveFileSize());
        fileManager.append(entry);
        System.out.println(fileManager.getActiveFileSize());
    }
}