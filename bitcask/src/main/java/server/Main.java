package server;

import java.util.ArrayList;

public class Main {
    public static void main(String[] args) {
        FileManager fileManager = new FileManager();
        fileManager.createDir();
        fileManager.createActiveFile();
        DataEntry entry = new DataEntry();
        ArrayList<Byte> a = new ArrayList<>();
        a.add((byte) 1);
        a.add((byte) 2);
        a.add((byte) 3);
        a.add((byte) 30);
        a.add((byte) 5);
        a.add((byte) 6);
        entry.createEntry(0, a);
        fileManager.log(entry);
        System.out.println(fileManager.getActiveFileSize());
        Address address = new Address();
        address.createAddress(0, a.size(), (int) fileManager.getActiveFileSize(), 0);
        fileManager.log(entry);
        System.out.println(fileManager.getActiveFileSize());
        DataEntry returned = fileManager.lookup(address);
        System.out.println(returned.getKey());
        System.out.println(returned.getValue()[0]);
        System.out.println(returned.getValue()[3]);
    }
}