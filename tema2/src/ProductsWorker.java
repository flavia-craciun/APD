// CRACIUN FLAVIA - MARIA
// 336CA

import java.io.BufferedReader;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class ProductsWorker implements Runnable {
    private ConcurrentHashMap ordersInfo;
    private BufferedReader reader;
    private String currentOrder;
    private CountDownLatch latch;

    public ProductsWorker(ConcurrentHashMap ordersInfo, BufferedReader reader, String currentOrder, CountDownLatch latch) {
        this.ordersInfo = ordersInfo;
        this.reader = reader;
        this.currentOrder = currentOrder;
        this.latch = latch;
    }

    private synchronized void writeFile(String str1, String str2) throws IOException {
        Tema2.productsWriter.write(str1 + "," + str2 + ",shipped" + "\n");
    }

    @Override
    public void run() {
        String line = null;
        try {
            line = reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (line != null) {
            String[] arrOfStr = line.split(",");
            if (arrOfStr[0].equals(currentOrder)) {
                latch.countDown(); // Mark task as completed
                try {
                    writeFile(arrOfStr[0], arrOfStr[1]); // Ship product
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                line = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
