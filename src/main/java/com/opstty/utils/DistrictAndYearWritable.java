package com.opstty.utils;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DistrictAndYearWritable implements Writable {
    private int district;
    private int year;

    public DistrictAndYearWritable() {}

    public DistrictAndYearWritable(int district, int year) {
        this.district = district;
        this.year = year;
    }

    public int getDistrict() {
        return district;
    }

    public void setDistrict(int district) {
        this.district = district;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(district);
        out.writeInt(year);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        district = in.readInt();
        year = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DistrictAndYearWritable)) return false;
        DistrictAndYearWritable that = (DistrictAndYearWritable) o;
        return district == that.district && year == that.year;
    }

    @Override
    public String toString() {
        return district + "\t" + year;
    }
}
