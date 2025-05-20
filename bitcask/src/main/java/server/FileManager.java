package server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileManager {

    private long activeFileID = -1;
    private File activeFile;
    private String path = "bitcask/src/main/resources/logs/";

    public void createDir() {
        try {
            Files.createDirectories(Paths.get(path));
        } catch (IOException e) {
            System.err.println("Failed to create directories for bitcask: " + e.getMessage());
            return;
        }
    }

    public void createNewFile() {
        this.activeFileID++;
        this.activeFile = new File(path + this.activeFileID + ".txt");
        try {
            this.activeFile.createNewFile();
        } catch (IOException e) {
            System.out.println("Can't create new log file # " + this.activeFileID + " for bitcask");
            e.printStackTrace();
        }
    }

    public void append(Entry entry) {
        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(activeFile, true);
            fileOutputStream.write(ByteBuffer.allocate(Long.BYTES).putLong(entry.getTimeStamp()).array());
            fileOutputStream.write(ByteBuffer.allocate(Integer.BYTES).putLong(entry.getKeySize()).array());
            fileOutputStream.write(ByteBuffer.allocate(Integer.BYTES).putLong(entry.getValueSzie()).array());
            fileOutputStream.write(ByteBuffer.allocate(Long.BYTES).putLong(entry.getKey()).array());
            byte[] data = entry.getValue();
            for (byte byte1 : data)
                fileOutputStream.write(byte1);
        } catch (IOException e) {
            System.out.println("Can't append to log file " + this.activeFileID);
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

    public void compact() {
        // TODO: implement compaction
    }

    public Entry lookup(Address address) {
        Entry entry = new Entry();
        File readFile = new File(this.path + address.getFileID() + ".txt");
        try (RandomAccessFile raf = new RandomAccessFile(readFile, "r")) {
            raf.seek(address.getValuePose());
            long timeStamp = retrieveNumber(raf, Long.BYTES).getLong();
            int keySize = (int)retrieveNumber(raf, Integer.BYTES).getInt();
            int valueSize = (int)retrieveNumber(raf, Integer.BYTES).getInt();
            long key = retrieveNumber(raf, Long.BYTES).getLong();
            byte[] value = retrieveValue(raf, valueSize);

            entry.setKeySize(keySize);
            entry.setTime(timeStamp);
            entry.setValueSize(valueSize);
            entry.setKey(key);
            entry.setValue(value);
        } catch (IOException e) {
            System.out.println("Can't lookup data");
            e.printStackTrace();
        }
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
            System.out.println("Can't retreive long");
            e.printStackTrace();
        }
        return bytes;
    }

}
