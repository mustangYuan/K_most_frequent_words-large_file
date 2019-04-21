package main;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Preprocess {
    private String filepath;
    private List<BufferedWriter> fileWriters;
    private List<String> files;
    private int numOfFile = 8;

    public Preprocess(String str) {
        filepath = str;
        fileWriters = new ArrayList<>();
        files = new ArrayList<>();
    }

    public List<String> partition() {
        File file = new File(filepath);
        String pathPrefix = file.getParent() + "/";

        //create files
        try {
            for (int i = 0; i < numOfFile; ++i) {
                String new_filepath = pathPrefix + "part_" + i + ".txt";
                files.add(new_filepath);
                File new_file = new File(new_filepath);
                if (!new_file.exists()) new_file.createNewFile();
                FileWriter fWriter = new FileWriter(new_file, true);
                BufferedWriter writer = new BufferedWriter(fWriter);
                fileWriters.add(writer);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        //use thread pool to manage threads
        ExecutorService threadPool = Executors.newFixedThreadPool(5);

        try {
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
            BufferedReader reader = new BufferedReader(new InputStreamReader(bis), 10 * 1024 * 1024);

            boolean hasMore = true;
            List<String> txt;
            String line = null;
            int numOfLines;

            //read 100 lines and allocate a thread to do partition
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

                Worker worker = new Worker(txt);
                threadPool.execute(worker);
            }
            reader.close();

            //wait for all threads to finish
            threadPool.shutdown();
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);

            for (BufferedWriter writer : fileWriters) {
                if (writer != null) writer.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return files;
    }

    private class Worker extends Thread {
        private List<String> text;
        private List<StringBuilder> strings;

        private Worker(List<String> text) {
            this.text = text;
            strings = new ArrayList<>();
            for (int i = 0; i < numOfFile; ++i)
                strings.add(new StringBuilder());
        }

        public void run() {
            //partition words into files according to their hash code
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
                        int index = (str.hashCode() % numOfFile + numOfFile) % numOfFile;
                        strings.get(index).append(str).append(" ");
                        sb.delete(0, sb.length());
                    }
                }
                if (sb.length() != 0) {
                    String str = sb.toString();
                    int index = (str.hashCode() % numOfFile + numOfFile) % numOfFile;
                    strings.get(index).append(str).append(" ");
                }
            }

            //write result to corresponding file
            for (int i = 0; i < numOfFile; ++i) {
                StringBuilder sb = strings.get(i);
                if (sb.length() != 0) {
                    try {
                        sb.deleteCharAt(sb.length() - 1);
                        //lock file while writing
                        synchronized (fileWriters.get(i)) {
                            fileWriters.get(i).write(sb.toString());
                            fileWriters.get(i).newLine();
                            fileWriters.get(i).flush();
                        }
                        sb.delete(0, sb.length());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }
}
