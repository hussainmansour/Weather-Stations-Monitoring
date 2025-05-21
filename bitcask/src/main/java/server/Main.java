package server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        FileManager fileManager = new FileManager();
        fileManager.createDir();
        fileManager.createActiveFile();
        Map<Long, Address> addresses = new HashMap<>();
        for (int i = 0; i < 15; i++) {
            DataEntry entry = new DataEntry();
            ArrayList<Byte> a = new ArrayList<>();
            a.add((byte) 1);
            a.add((byte) i);
            Address address = new Address();
            address.createAddress(i % 3, a.size(), (int) fileManager.getActiveFileSize(), 0);
            addresses.put((long) i % 3, address);
            entry.createEntry(i % 3, a);
            fileManager.log(entry);
            if (i % 3 == 2) {
                fileManager.createActiveFile();
            }
        }

        for (Map.Entry<Long, Address> address : addresses.entrySet()) {
            DataEntry returned = fileManager.lookup(address.getValue());
            System.out.println(returned.getValue()[1]);
        }
    }
}