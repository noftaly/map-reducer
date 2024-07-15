package com.opstty.utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DistrictAndCountWritable implements Writable {
    private int district;
    private int count;

    public DistrictAndCountWritable() {}

    public DistrictAndCountWritable(int district, int count) {
        this.district = district;
        this.count = count;
    }

    public int getDistrict() {
        return district;
    }

    public void setDistrict(int district) {
        this.district = district;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(district);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        district = in.readInt();
        count = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DistrictAndCountWritable)) return false;
        DistrictAndCountWritable that = (DistrictAndCountWritable) o;
        return district == that.district && count == that.count;
    }

    @Override
    public String toString() {
        return district + "\t" + count;
    }
}
