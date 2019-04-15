package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class KWords {
    private int k = 100;
    private String filepath;
    private ConcurrentHashMap<String, AtomicInteger> count;
    List<CountDownLatch> latch;


    public KWords(String str) {
        filepath = str;
        count = new ConcurrentHashMap<>();
        latch = new ArrayList<>();
    }

    private void findFreq() {
        BufferedReader reader = null;
        ExecutorService threadPool = Executors.newFixedThreadPool(7);

        try {
            reader = new BufferedReader(new FileReader(filepath));
            boolean hasMore = true;
            int i = 0;
            List<String> txt;
            String line = null;
            int numOfLines;

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

                latch.add(new CountDownLatch(1));
                WordCounter counter = new WordCounter(txt, i);
                threadPool.execute(counter);
                i++;
            }
            reader.close();

            for (CountDownLatch l : latch) {
                l.await();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            threadPool.shutdown();
        }
    }

    private void kMostFrequent() {
        System.out.println(count.size() + " total different words.");

        PriorityQueue<Word> pq = new PriorityQueue<>();
        for (Map.Entry<String, AtomicInteger> entry : count.entrySet()) {
            String word = entry.getKey();
            int freq = entry.getValue().get();
            pq.add(new Word(word, freq));
            if (pq.size() > k) pq.poll();
        }

        Stack<Word> s = new Stack<>();
        while (pq.size() != 0) s.push(pq.poll());

        System.out.println("The first 100 most frequent words:");

        int i = 1;
        while (s.size() != 0) {
            Word w = s.pop();
            System.out.printf("%3d: %-20s %10d\n", i, w.str, w.freq);
            i++;
        }
    }

    public void showResult() {
        long startTime = System.currentTimeMillis();
        findFreq();
        long endTime = System.currentTimeMillis();
        System.out.println("Execution time: " + (endTime - startTime) / 1000.0 + "s");
        kMostFrequent();
    }

    private class Word implements Comparable<Word> {
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
        private int index;
        private List<String> text;
        private Map<String, Integer> cnt;

        private WordCounter(List<String> text, int i) {
            this.text = text;
            index = i;
            cnt = new HashMap<>();
        }

        public void run() {
            counter();

            for (Map.Entry<String, Integer> entry : cnt.entrySet()) {
                String key = entry.getKey();
                int value = entry.getValue();

                if (count.putIfAbsent(key, new AtomicInteger(value)) != null)
                    count.get(key).getAndAdd(value);

            }

            latch.get(index).countDown();
        }

        private void counter() {
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
                        sb = new StringBuilder();
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
        //KWords k_words = new KWords("C:\\Users\\28939\\Desktop\\2019Spring\\COEN242 Big Data\\data_1GB.txt");
        KWords k_words = new KWords("C:\\Users\\28939\\Downloads\\DataSet\\data_8GB.txt");
        k_words.showResult();

//        if (args.length == 0) return;
//
//        for (int i = 0; i < args.length; ++i) {
//            KWords k_words = new KWords(args[i]);
//            k_words.showResult();
//        }

    }
}
