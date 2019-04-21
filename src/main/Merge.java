package main;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class Merge {
    private List<String> files;
    private int k = 100;

    public Merge(List<String> list) {
        files = list;
    }

    public void showResult() {
        //use a max heap to do merge sort, and just take the first k words
        List<BufferedReader> bReaders = new ArrayList<>();
        PriorityQueue<Word> maxHeap = new PriorityQueue<>();

        //put the first word of each file to the heap
        try {
            int i = 0;
            for (String fname : files) {
                BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(fname)));
                BufferedReader reader = new BufferedReader(new InputStreamReader(bis));
                bReaders.add(reader);
                String line = reader.readLine();
                String[] temp = line.split(" ");
                String str = temp[0];
                int freq = Integer.parseInt(temp[1]);
                Word word = new Word(str, freq, i);
                maxHeap.add(word);
                i++;
            }

            //do merge sort and display result
            System.out.println("The first 100 most frequent words:");
            i = 0;
            while (i != k) {
                i++;
                Word word = maxHeap.poll();
                System.out.printf("%3d: %-20s %10d\n", i, word.str, word.freq);
                int index = word.index;
                String line = bReaders.get(index).readLine();
                String[] temp = line.split(" ");
                String str = temp[0];
                int freq = Integer.parseInt(temp[1]);
                word = new Word(str, freq, index);
                maxHeap.add(word);
            }

            for (BufferedReader br : bReaders) {
                if (br != null) br.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class Word implements Comparable<Word> {
        public String str;
        public int freq;
        public int index;

        public Word(String str, int freq, int index) {
            this.str = str;
            this.freq = freq;
            this.index = index;
        }

        //make the priority queue a max heap
        public int compareTo(Word w) {
            return w.freq - this.freq;
        }
    }
}
