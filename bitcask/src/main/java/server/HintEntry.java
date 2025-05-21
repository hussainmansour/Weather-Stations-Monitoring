package server;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class HintEntry extends Entry {
    private int valuePose;

    public void setValuePose(int valuePose){
        this.valuePose = valuePose;
    }

    public int getValuePose(){
        return this.valuePose;
    }

    @Override
    public byte[] serializeAll() {
        ArrayList<Byte> metaData = super.serialize();
        byte[] data = ByteBuffer.allocate(Integer.BYTES).putInt(this.valuePose).array();
        byte[] allData = new byte[metaData.size() + Integer.BYTES];
        super.setWriteSize(metaData.size() + Integer.BYTES);
        for (int i = 0; i < metaData.size(); i++) {
            allData[i] = metaData.get(i);
        }
        System.arraycopy(data, 0, allData, metaData.size(), data.length);
        return allData;
    }

}
