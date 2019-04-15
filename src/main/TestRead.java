package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class TestRead {
    public void run() {
        String filepath = "C:\\Users\\28939\\Desktop\\2019Spring\\COEN242 Big Data\\data_1GB.txt";
        String line;
        Map<String, Integer> freq = new HashMap<>();
        try {
            FileReader fr = new FileReader(filepath);
            BufferedReader reader = new BufferedReader(fr);
            int i = 0;
            while((line = reader.readLine())!=null) {
                i++;
            }

            fr.close();
            System.out.println(i);
//            String[] words = line.split(" ");
//            for (String w : words) {
//                if (freq.containsKey(w))
//                    freq.put(w, freq.get(w) + 1);
//                else
//                    freq.put(w, 1);
//            }
//            for (Map.Entry<String, Integer> entry : freq.entrySet()) {
//                System.out.println(entry.getKey() + ": " + entry.getValue());
//            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
//        TestRead test = new TestRead();
//        test.run();
        System.out.printf("%3d: %-20s %10d",26,"word",5896752);
    }
}
