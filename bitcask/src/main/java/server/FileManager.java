package server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class FileManager {

    private long lastFileID = -1;
    private long activeFileID = -1;
    private long fileToCompact = 0;
    private File activeFile;
    private String path = "bitcask/src/main/resources/logs/";

    public void createDir() {
        try {
            Files.createDirectories(Paths.get(path));
            System.out.println("Bitcask directory created");
        } catch (IOException e) {
            System.err.println("Failed to create directories for bitcask: " + e.getMessage());
            return;
        }
    }

    public void createActiveFile() {
        this.lastFileID++;
        this.activeFileID = this.lastFileID;
        this.activeFile = new File(path + this.activeFileID + ".data");
        try {
            this.activeFile.createNewFile();
            System.out.println("New active file created: " + this.activeFile);
            if (this.activeFileID != 0)
                createHintFile(this.activeFileID - 1);
        } catch (IOException e) {
            System.out.println("Can't create new active file # " + this.activeFileID + " for bitcask");
            e.printStackTrace();
        }
    }

    private void createHintFile(long fileID) {
        File dataFile = new File(this.path + fileID + ".data");
        ArrayList<Entry> hintEntries = scanForHints(dataFile);
        File hintFile = new File(this.path + fileID + ".hint");
        for (Entry entry : hintEntries) {
            append(entry, hintFile);
        }
        System.out.println("Hint file created: " + hintFile);
    }

    private ArrayList<Entry> scanForHints(File dataFile) {
        Map<Long, DataEntry> lastEntries = new HashMap<>();
        Map<Long, Long> lastPoses = new HashMap<>();
        try (RandomAccessFile raf = new RandomAccessFile(dataFile, "r")) {
            long pose = raf.getFilePointer();
            while (pose < raf.length()) {
                DataEntry newEntry = readDataEntry(raf);
                lastEntries.put(newEntry.getKey(), newEntry);
                lastPoses.put(newEntry.getKey(), pose);
                pose = raf.getFilePointer();
            }
        } catch (IOException e) {
            System.out.println("Can't read data file to create hints");
            e.printStackTrace();
        }
        ArrayList<Entry> hintEntries = new ArrayList<>();
        for (Map.Entry<Long, DataEntry> me : lastEntries.entrySet()) {
            Entry hintEntry = formHint(me.getValue(), lastPoses.get(me.getKey()));
            hintEntries.add(hintEntry);
        }
        return hintEntries;
    }

    private Entry formHint(DataEntry entry, long pose) {
        HintEntry hintEntry = new HintEntry();
        hintEntry.setTime(entry.getTimeStamp());
        hintEntry.setKey(entry.getKey());
        hintEntry.setKeySize(entry.getKeySize());
        hintEntry.setValueSize(entry.getValueSize());
        hintEntry.setValuePose((int) pose);
        return hintEntry;
    }

    public long getActiveFileID() {
        return this.activeFileID;
    }

    public Address log(DataEntry entry) {
        Address address = convertEntryToAddress(entry, getActiveFileSize(), activeFileID);
        append(entry, activeFile);
        return address;
    }

    private void append(Entry entry, File file) {
        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(file, true);
            byte[] dataToWrite = entry.serializeAll();
            fileOutputStream.write(ByteBuffer.allocate(entry.getWriteSize()).put(dataToWrite).array());
            fileOutputStream.close();
        } catch (IOException e) {
            System.out.println("Can't append to " + file);
            e.printStackTrace();
        }
    }

    public long getActiveFileSize() {
        try {
            return this.activeFile.length();
        } catch (Exception e) {
            System.out.println("No current active file");
            return 0;
        }
    }

    public DataEntry lookup(Address address) {
        File readFile = new File(this.path + address.getFileID() + ".data");
        DataEntry entry = null;
        try (RandomAccessFile raf = new RandomAccessFile(readFile, "r")) {
            raf.seek(address.getValuePose());
            entry = readDataEntry(raf);
        } catch (IOException e) {
            System.out.println("Can't lookup data");
            e.printStackTrace();
        }
        return entry;
    }

    private DataEntry readDataEntry(RandomAccessFile raf) {
        long timeStamp = retrieveNumber(raf, Long.BYTES).getLong();
        int keySize = retrieveNumber(raf, Integer.BYTES).getInt();
        int valueSize = retrieveNumber(raf, Integer.BYTES).getInt();
        long key = retrieveNumber(raf, keySize).getLong();
        byte[] value = retrieveValue(raf, valueSize);

        DataEntry entry = new DataEntry();
        entry.setKeySize(keySize);
        entry.setTime(timeStamp);
        entry.setValueSize(valueSize);
        entry.setKey(key);
        entry.setValue(value);
        return entry;
    }

    private ByteBuffer retrieveNumber(RandomAccessFile raf, int size) {
        byte[] bytes = new byte[size];
        try {
            raf.read(bytes);
        } catch (IOException e) {
            System.out.println("Can't retreive long");
            e.printStackTrace();
        }
        return ByteBuffer.wrap(bytes);
    }

    private byte[] retrieveValue(RandomAccessFile raf, int valueSize) {
        byte[] bytes = new byte[valueSize];
        try {
            raf.read(bytes);
        } catch (IOException e) {
            System.out.println("Can't retreive value");
            e.printStackTrace();
        }
        return bytes;
    }

    public Map<Long, Address> compact() {
        System.out.println("Start compaction from file: " + fileToCompact);
        Map<Long, Entry> summary = new HashMap<>();
        for (; fileToCompact < lastFileID; fileToCompact++) {
            ArrayList<HintEntry> hints = readHintFile(fileToCompact);
            for (HintEntry hintEntry : hints) {
                Address address = convertEntryToAddress(hintEntry, hintEntry.getValuePose(), fileToCompact);
                DataEntry dataEntry = lookup(address);
                long key = dataEntry.getKey();
                if (summary.get(key) == null || summary.get(key).getTimeStamp() < dataEntry.getTimeStamp())
                    summary.put(key, dataEntry);
            }
        }
        lastFileID++;
        File compactionFile = new File(path + lastFileID + ".data");
        Map<Long, Address> newAddresses = new HashMap<>();
        for (Map.Entry<Long, Entry> entry : summary.entrySet()) {
            append(entry.getValue(), compactionFile);
            newAddresses.put(entry.getKey(),
                    convertEntryToAddress(entry.getValue(), compactionFile.length(), lastFileID));
        }
        System.out.println("Compaction is done in file " + compactionFile);
        createHintFile(lastFileID);
        return newAddresses;
    }

    private Address convertEntryToAddress(Entry entry, long valuePose, long fileID) {
        Address address = new Address();
        address.createAddress(fileID, entry.getValueSize(), (int) valuePose, entry.getTimeStamp());
        return address;
    }

    private ArrayList<HintEntry> readHintFile(long fileToCompact) {
        File hintFile = new File(path + fileToCompact + ".hint");
        ArrayList<HintEntry> hintEntries = new ArrayList<>();
        try (RandomAccessFile raf = new RandomAccessFile(hintFile, "r")) {
            while (raf.getFilePointer() < raf.length()) {
                HintEntry entry = readHintEntry(raf);
                hintEntries.add(entry);
            }
        } catch (IOException e) {
            System.out.println("Can't read hint from file to compact");
            e.printStackTrace();
        }
        return hintEntries;
    }

    private HintEntry readHintEntry(RandomAccessFile raf) {
        long timeStamp = retrieveNumber(raf, Long.BYTES).getLong();
        int keySize = retrieveNumber(raf, Integer.BYTES).getInt();
        int valueSize = retrieveNumber(raf, Integer.BYTES).getInt();
        long key = retrieveNumber(raf, keySize).getLong();
        int valPose = retrieveNumber(raf, Integer.BYTES).getInt();

        HintEntry hintEntry = new HintEntry();
        hintEntry.setKeySize(keySize);
        hintEntry.setTime(timeStamp);
        hintEntry.setValueSize(valueSize);
        hintEntry.setKey(key);
        hintEntry.setValuePose(valPose);
        return hintEntry;
    }

}
