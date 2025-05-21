package server;

public class Address {
    private long fileID;
    private int valueSize;
    private int valuePose;
    private long timeStamp;

    public Address createAddress(long fileID, int valueSize, int valuePose, long timeStamp) {
        this.fileID = fileID;
        this.valueSize = valueSize;
        this.valuePose = valuePose;
        this.timeStamp = timeStamp;
        return this;
    }

    public long getFileID() {
        return this.fileID;
    }

    public long getValueSize() {
        return this.valueSize;
    }

    public long getValuePose() {
        return this.valuePose;
    }

    public long getTimeStamp() {
        return this.timeStamp;
    }
}
