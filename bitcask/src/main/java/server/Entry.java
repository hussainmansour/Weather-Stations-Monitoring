package server;

import java.util.ArrayList;

public class Entry {
    private long timeStamp;
    private int keySize;
    private int valueSize;
    private long key;
    private byte[] value;

    public Entry createEntry(long key, ArrayList<Byte> value) {
        this.timeStamp = System.currentTimeMillis();
        this.key = key;
        copyBytesList(value);
        this.keySize = Long.BYTES;
        this.valueSize = value.size();

        return this;
    }

    private void copyBytesList(ArrayList<Byte> bytes){
        this.value = new byte[bytes.size()];
        for(int i = 0; i < bytes.size(); i++){
            this.value[i] = bytes.get(i);
        }
    }

    public long getTimeStamp() {
        return this.timeStamp;
    }

    public int getKeySize() {
        return this.keySize;
    }

    public long getKey() {
        return this.key;
    }

    public int getValueSzie() {
        return this.valueSize;
    }

    public byte[] getValue() {
        return this.value;
    }

    public void setTime(long timeStamp){
        this.timeStamp = timeStamp;
    }

    public void setKeySize(int keySize){
        this.keySize = keySize;
    }

    public void setValueSize(int valueSize){
        this.valueSize = valueSize;
    }

    public void setKey(long key){
        this.key = key;
    }

    public void setValue(byte[] value){
        this.value = value;
    }

}
