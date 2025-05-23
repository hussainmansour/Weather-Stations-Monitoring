package com.example;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class DataEntry extends Entry {

    private byte[] value;

    public void setValue(byte[] value){
        this.value = value;
    }

    public byte[] getValue(){
        return this.value;
    }

    public DataEntry createEntry(long key, ArrayList<Byte> value) {
        super.setTime(System.currentTimeMillis());
        super.setKey(key);
        super.setKeySize(Long.BYTES);
        super.setValueSize(value.size());
        copyBytesList(value);
        return this;
    }

    private void copyBytesList(ArrayList<Byte> bytes) {
        this.value = new byte[bytes.size()];
        for (int i = 0; i < bytes.size(); i++) {
            this.value[i] = bytes.get(i);
        }
    }

    @Override
    public byte[] serializeAll() {
        ArrayList<Byte> metaData = super.serialize();
        byte[] data = ByteBuffer.allocate(super.getValueSize()).put(this.value).array();
        byte[] allData = new byte[metaData.size() + super.getValueSize()];
        super.setWriteSize(metaData.size() + super.getValueSize());
        for (int i = 0; i < metaData.size(); i++) {
            allData[i] = metaData.get(i);
        }
        System.arraycopy(data, 0, allData, metaData.size(), data.length);
        return allData;
    }

}
