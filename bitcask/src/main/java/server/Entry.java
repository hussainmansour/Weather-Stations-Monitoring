package server;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public abstract class Entry {
    private long timeStamp;
    private int keySize;
    private int valueSize;
    private long key;

    private int writeSize;

    public long getTimeStamp() {
        return this.timeStamp;
    }

    public int getKeySize() {
        return this.keySize;
    }

    public long getKey() {
        return this.key;
    }

    public int getValueSize() {
        return this.valueSize;
    }

    public void setTime(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }

    public void setValueSize(int valueSize) {
        this.valueSize = valueSize;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public void setWriteSize(int size){
        this.writeSize = size;
    }

    public int getWriteSize(){
        return this.writeSize;
    }

    protected ArrayList<Byte> serialize() {
        ArrayList<Byte> data = new ArrayList<>();
        byte[] time = ByteBuffer.allocate(Long.BYTES).putLong(this.timeStamp).array();
        byte[] ksize = ByteBuffer.allocate(Integer.BYTES).putInt(this.keySize).array();
        byte[] valsize = ByteBuffer.allocate(Integer.BYTES).putInt(this.valueSize).array();
        byte[] k = ByteBuffer.allocate(keySize).putLong(this.key).array();
        for (byte b : time) {
            data.add(b);
        }
        for (byte b : ksize) {
            data.add(b);
        }
        for (byte b : valsize) {
            data.add(b);
        }
        for (byte b : k) {
            data.add(b);
        }
        return data;
    }

    public abstract byte[] serializeAll();

}
