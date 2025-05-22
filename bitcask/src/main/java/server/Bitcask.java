package server;

import java.util.ArrayList;
import java.util.Map;

public class Bitcask {

    private Map<Long, Address> table;
    private Bitcask instance;
    private FileManager fileManager;

    private Bitcask() {
        fileManager = new FileManager();
        fileManager.createDir();
        fileManager.createActiveFile();
    }

    public Bitcask getBitcaskInstance() {
        if (instance == null)
            instance = new Bitcask();
        return instance;
    }

    public void add(DataEntry dataEntry) {
        Address newAddress = fileManager.log(dataEntry);
        table.put(dataEntry.getKey(), newAddress);
    }

    public DataEntry get(long key) {
        return fileManager.lookup(table.get(key));
    }

    public ArrayList<DataEntry> getAll() {
        ArrayList<DataEntry> entries = new ArrayList<>();
        for (Map.Entry<Long, Address> mapEntry : table.entrySet()) {
            entries.add(fileManager.lookup(mapEntry.getValue()));
        }
        return entries;
    }

}
