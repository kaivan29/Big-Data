package com.refactorlabs.cs378.assign2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Kaivan on 9/11/2016.
 */
public class WordStatisticsWritable implements Writable {

    private long wordCount;
    private double paragraphCount;
    private double doubleOfSquares;
    private long total_words;

    public WordStatisticsWritable(){
        wordCount = 0L;
        paragraphCount = 0D;
        doubleOfSquares = 0D;
        total_words = 0L;
    }
/*    public WordStatisticsWritable(long wc,double pc, double dos){
        wordCount = wc;
        paragraphCount = pc;
        doubleOfSquares = dos;
    }
*/
    public void set_wordCount(long wc){
        wordCount = wc;
    }

    public void set_paragraphCount(double pc){
        paragraphCount = pc;
    }

    public void set_doubleOfSquares(double dos){
        doubleOfSquares = dos;
    }

    public double get_paragraphCount(){
        return paragraphCount;
    }

    public long get_wordCount(){
        return wordCount;
    }

    public double get_doubleOfSquares(){
        return doubleOfSquares;
    }

    public long get_totalWords(){
        return total_words;
    }

    @Override
    public void write(DataOutput out) throws IOException{
        out.writeLong(wordCount);
        out.writeDouble(paragraphCount);
        out.writeDouble(doubleOfSquares);
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        wordCount = in.readLong();
        paragraphCount = in.readDouble();
        doubleOfSquares = in.readDouble();
    }

    @Override
    public String toString() {
        return "\t"+wordCount+","+paragraphCount+","+doubleOfSquares;
    }
}



