package main;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class KWords {
    private int k = 100;    //number of words needed
    private String filepath;
    private ConcurrentHashMap<String, AtomicInteger> count;
    private Stack<Word> s;  //store the most frequent k words

    public KWords(String str) {
        filepath = str;
        count = new ConcurrentHashMap<>();
        s = new Stack<>();
    }

    private void findFreq() {
        //find frequency of each word in a file

        //use thread pool to manage threads
        ExecutorService threadPool = Executors.newFixedThreadPool(11);

        try {
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(filepath)));
            BufferedReader reader = new BufferedReader(new InputStreamReader(bis), 10 * 1024 * 1024);

            boolean hasMore = true;
            List<String> txt;
            String line = null;
            int numOfLines;

            //read 100 lines and allocate a thread to do word count
            while (hasMore) {
                numOfLines = 100;
                txt = new ArrayList<>();

                while (numOfLines > 0) {
                    if ((line = reader.readLine()) != null)
                        txt.add(line);
                    else {
                        hasMore = false;
                        break;
                    }
                    numOfLines--;
                }

                WordCounter counter = new WordCounter(txt);
                threadPool.execute(counter);
            }
            reader.close();

            //wait for all threads to finish
            threadPool.shutdown();
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void kMostFrequent() {
        //traverse each entry in the map, and use a min heap to maintain the k most frequent words
        PriorityQueue<Word> minHeap = new PriorityQueue<>();
        for (Map.Entry<String, AtomicInteger> entry : count.entrySet()) {
            String word = entry.getKey();
            int freq = entry.getValue().get();

            //keep at most k elements in the heap
            if (minHeap.size() < k) minHeap.add(new Word(word, freq));
            else {
                if (freq > minHeap.peek().freq) {
                    minHeap.poll();
                    minHeap.add(new Word(word, freq));
                }
            }
        }

        //store the result in a stack
        while (minHeap.size() != 0) s.push(minHeap.poll());
    }

    public void calculate() {
        findFreq();
        kMostFrequent();
    }

    private String writeToFile() {
        //write the result to a file for future merge
        File f = new File(filepath);
        String pathPrefix = f.getParent() + "/";
        String filename = pathPrefix + "res_" + filepath.substring(filepath.lastIndexOf('_') + 1);

        try {
            File file = new File(filename);
            if (!file.exists()) file.createNewFile();
            FileWriter fWriter = new FileWriter(file, true);
            BufferedWriter writer = new BufferedWriter(fWriter);
            StringBuilder sb = new StringBuilder();

            while (!s.empty()) {
                Word w = s.pop();
                sb.append(w.str).append(' ').append(w.freq);
                writer.write(sb.toString());
                writer.newLine();
                sb.delete(0, sb.length());
            }

            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return filename;
    }

    private void showResult() {
        //display the result
        System.out.println("The first 100 most frequent words:");

        int i = 1;
        while (!s.empty()) {
            Word w = s.pop();
            System.out.printf("%3d: %-20s %10d\n", i, w.str, w.freq);
            i++;
        }
    }

    private class Word implements Comparable<Word> {
        //used in min heap
        public String str;
        public int freq;

        public Word(String str, int freq) {
            this.str = str;
            this.freq = freq;
        }

        public int compareTo(Word w) {
            return this.freq - w.freq;
        }
    }

    private class WordCounter extends Thread {
        private List<String> text;
        private Map<String, Integer> cnt;

        private WordCounter(List<String> text) {
            this.text = text;
            cnt = new HashMap<>();
        }

        public void run() {
            counter();

            //update record to main thread
            for (Map.Entry<String, Integer> entry : cnt.entrySet()) {
                String key = entry.getKey();
                int value = entry.getValue();

                if (count.putIfAbsent(key, new AtomicInteger(value)) != null)
                    count.get(key).getAndAdd(value);
            }

        }

        private void counter() {
            //count words frequency, and store in a local hash map
            for (String line : text) {
                int n = line.length();
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < n; ++i) {
                    char c = line.charAt(i);
                    if (c != ' ')
                        sb.append(c);
                    else {
                        if (sb.length() == 0) continue;
                        String str = sb.toString();
                        if (cnt.containsKey(str))
                            cnt.put(str, cnt.get(str) + 1);
                        else
                            cnt.put(str, 1);
                        sb.delete(0, sb.length());
                    }
                }
                if (sb.length() != 0) {
                    String str = sb.toString();
                    if (cnt.containsKey(str))
                        cnt.put(str, cnt.get(str) + 1);
                    else
                        cnt.put(str, 1);
                }
            }
        }

    }

    public static void main(String[] args) {
        //for 32GB file, do pre-process to partition the file first

        /*
        long startTime = System.currentTimeMillis();

        //use your file path
        Preprocess pre = new Preprocess("C:\\Users\\28939\\Downloads\\DataSet\\data_32GB.txt");
        List<String> files = pre.partition();

        long middleTime = System.currentTimeMillis();
        System.out.println("Pre-processing time: " + (middleTime - startTime) / 1000.0 + "s");

        List<String> resFiles = new ArrayList<>();
        for (String file : files) {
            KWords k_words = new KWords(file);
            k_words.calculate();
            String resFile = k_words.writeToFile();
            resFiles.add(resFile);
        }

        Merge merge = new Merge(resFiles);
        merge.showResult();

        long endTime = System.currentTimeMillis();
        System.out.println("Processing time: " + (endTime - middleTime) / 1000.0 + "s");

        //delete all files created during this program
        for (String filename : files) {
            File file = new File(filename);
            if (file.exists() && file.isFile())
                file.delete();
        }

        for (String resFile : resFiles) {
            File file = new File(resFile);
            if (file.exists() && file.isFile())
                file.delete();
        }

        */



        //for 1GB file and 8GB file, don't need to do pre-process

        long startTime = System.currentTimeMillis();

        //use your file path
        KWords k_words = new KWords("C:\\Users\\28939\\Downloads\\DataSet\\data_1GB.txt");
        k_words.calculate();
        k_words.showResult();

        long endTime = System.currentTimeMillis();
        System.out.println("Processing time: " + (endTime - startTime) / 1000.0 + "s");
    }
}
