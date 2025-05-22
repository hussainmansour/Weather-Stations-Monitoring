package server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Bitcask {

    private Map<Long, Address> table;
    private static Bitcask instance;
    private FileManager fileManager;
    private SystemKeeper systemKeeper;

    private Bitcask() {
        table = new HashMap<>();
        fileManager = new FileManager();
        fileManager.createDir();
        fileManager.createActiveFile();
        systemKeeper = new SystemKeeper(fileManager);
        systemKeeper.startInBackground();
    }

    public static Bitcask getBitcaskInstance() {
        if (instance == null)
            instance = new Bitcask();
        return instance;
    }

    public void add(DataEntry dataEntry) {
        Address newAddress = fileManager.log(dataEntry);
        long key = dataEntry.getKey();
        table.put(key, newAddress);
    }

    public DataEntry get(long key) {
        Address address = table.get(key);
        DataEntry dataEntry = fileManager.lookup(address);
        return dataEntry;
    }

    public ArrayList<DataEntry> getAll() {
        ArrayList<DataEntry> entries = new ArrayList<>();
        for (Map.Entry<Long, Address> mapEntry : table.entrySet()) {
            Address address = mapEntry.getValue();
            DataEntry dataEntry = fileManager.lookup(address);
            entries.add(dataEntry);
        }
        return entries;
    }

}
