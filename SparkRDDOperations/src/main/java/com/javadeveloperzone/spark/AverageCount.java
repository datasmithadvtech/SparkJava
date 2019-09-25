package com.javadeveloperzone.spark;

import java.io.Serializable;

public class AverageCount implements Serializable {

    public AverageCount (int total, int num) {
        this.total = total;
        this.num = num;
    }
    public int total;
    public int num;
    public double avg() {
        return total / (double) num;
    }

}
