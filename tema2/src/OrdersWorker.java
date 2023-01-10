// CRACIUN FLAVIA - MARIA
// 336CA

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

public class OrdersWorker extends Thread {
    private ConcurrentHashMap ordersInfo;
    private BufferedReader reader;
    private ExecutorService executor;

    public OrdersWorker(ConcurrentHashMap ordersInfo, BufferedReader reader, ExecutorService executor) {
        this.ordersInfo = ordersInfo;
        this.reader = reader;
        this.executor = executor;
    }

    private synchronized void writeFile(String str1, String str2) throws IOException {
        Tema2.ordersWriter.write(str1 + "," + str2 + ",shipped" + "\n");
    }

    private void callProductWorker(String orderId, int nrOfProducts) {
        BufferedReader productsReader = null;
        try {
            productsReader = new BufferedReader(new FileReader(Tema2.products.toString()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        CountDownLatch latch = new CountDownLatch(nrOfProducts);

        // Create a task for every product in the order
        for (int i = 0; i < nrOfProducts; i++)
            executor.submit(new ProductsWorker(ordersInfo, productsReader, orderId, latch));

        try {
            latch.await(); // Wait for level 2 threads to finish all tasks
            try {
                writeFile(orderId, Integer.toString(nrOfProducts)); // Ship order
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {

        String line = null;
        try {
            line = reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Read order and add its id and nr of products to the map
        while (line != null) {
            String[] arrOfStr = line.split(",");
            if (!arrOfStr[1].equals("0")) {
                ordersInfo.put(arrOfStr[0], Integer.parseInt(arrOfStr[1]));
                callProductWorker(arrOfStr[0], Integer.parseInt(arrOfStr[1]));
            }
            try {
                line = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
