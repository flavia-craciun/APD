// CRACIUN FLAVIA - MARIA
// 336CA

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.*;

public class Tema2 {
    public static FileWriter ordersWriter;
    public static FileWriter productsWriter;
    public static int maxThreads;
    public static StringBuilder products;

    public static void main(String[] args) throws IOException, InterruptedException {
        ordersWriter = new FileWriter("orders_out.txt");
        productsWriter = new FileWriter("order_products_out.txt");

        // Create output files paths
        StringBuilder orders = new StringBuilder(args[0]);
        orders.append("/orders.txt");
        products = new StringBuilder(args[0]);
        products.append("/order_products.txt");

        maxThreads = Integer.parseInt(args[1]);

        ConcurrentHashMap<String, Integer> ordersInfo = new ConcurrentHashMap<>();
        BufferedReader ordersReader = new BufferedReader(new FileReader(orders.toString()));

        // Level 2 threads pool
        ExecutorService executor = Executors.newFixedThreadPool(maxThreads);

        Thread[] t = new Thread[maxThreads];

        // Level 1 threads
        for (int i = 0; i < maxThreads; ++i) {
            t[i] = new OrdersWorker(ordersInfo, ordersReader, executor);
            t[i].start();
        }

        for (int i = 0; i < maxThreads; ++i) {
            try {
                t[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Wait for level 1 threads to finish before closing files
        Thread.sleep(5000);
        ordersWriter.close();
        productsWriter.close();
        executor.shutdown();
    }
}