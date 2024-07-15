package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class, "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districts", Districts.class, "A map/reduce program that counts the distinct districts in which there are remarkable trees.");
            programDriver.addClass("species", Species.class, "A map/reduce program that displays the different species of trees.");
            programDriver.addClass("kinds", Kinds.class, "A map/reduce program that displays the number of trees by kinds.");
            programDriver.addClass("tallest", Tallest.class, "A map/reduce program that displays the tallest tree of each kind.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
