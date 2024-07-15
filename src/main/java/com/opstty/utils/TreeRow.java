package com.opstty.utils;

import java.util.Arrays;

public class TreeRow {
    private final int district;
    private final String kind;
    private final String species;
    private final int year;
    private final float height;
    private final String objectId;

    public TreeRow(String row) {
        String[] fields = row.split(";");
        this.district = Integer.parseInt(fields[1]);
        this.kind = fields[2];
        this.species = fields[3];
        this.year = Integer.parseInt(fields[5]);
        this.height = Float.parseFloat(fields[6]);
        this.objectId = fields[11];
    }

    public int getDistrict() {
        return district;
    }

    public String getKind() {
        return kind;
    }

    public String getSpecies() {
        return species;
    }

    public int getYear() {
        return year;
    }

    public float getHeight() {
        return height;
    }

    public String getObjectId() {
        return objectId;
    }

    @Override
    public String toString() {
        return "TreeRow{" +
                "district=" + district +
                ", kind='" + kind + '\'' +
                ", species='" + species + '\'' +
                ", year=" + year +
                ", height=" + height +
                ", objectId='" + objectId + '\'' +
                '}';
    }
}
