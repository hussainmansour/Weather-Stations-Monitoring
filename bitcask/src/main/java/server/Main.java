package server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        FileManager fileManager = new FileManager();
        fileManager.createDir();
        fileManager.createActiveFile();
        long cur = 0;
        Map<Long, Address> addresses = new HashMap<>();
        for (int i = 0; i < 15; i++) {
            DataEntry entry = new DataEntry();
            ArrayList<Byte> a = new ArrayList<>();
            a.add((byte) 1);
            a.add((byte) i);
            Address address = new Address();
            address.createAddress(cur, a.size(), (int) fileManager.getActiveFileSize(), System.currentTimeMillis());
            addresses.put((long) i % 3, address);
            entry.createEntry(i % 3, a);
            fileManager.log(entry);
            if(i == 5 || i == 9){
                Map<Long, Address> m =  fileManager.compact();
                for(Map.Entry<Long, Address> d : m.entrySet()){
                    addresses.put(d.getKey(), d.getValue());
                }
            }
            if (i % 3 == 2) {
                fileManager.createActiveFile();
                cur = fileManager.getActiveFileID();
            }
        }

        for (Map.Entry<Long, Address> address : addresses.entrySet()) {
            DataEntry returned = fileManager.lookup(address.getValue());
            System.out.println("add " + address.getValue().getFileID());
            System.out.println(returned.getValue()[1]);
        }
    }
}