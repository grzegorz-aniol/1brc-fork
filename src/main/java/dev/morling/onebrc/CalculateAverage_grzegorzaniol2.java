/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CalculateAverage_grzegorzaniol2 {

    private static final String FILE = "./measurements.txt";
    private static final int CHUNK_LINES = 100_000;
    private static final int BUFF_SIZE = 15 * CHUNK_LINES;
    private static final int WORKERS = 1;
    private static final int BUFFERS_COUNT = WORKERS + 1;
    private static final ByteBuffer[] BUFFERS = new ByteBuffer[BUFFERS_COUNT];
    private static final ByteBuffer END_BUF = ByteBuffer.allocate(0);

    private static final LongAccumulator timeRead = new LongAccumulator(Long::sum, 0L);
    private static final LongAccumulator waitRead = new LongAccumulator(Long::sum, 0L);
    private static final LongAccumulator timeCheck = new LongAccumulator(Long::sum, 0L);
    private static final LongAccumulator waitCheck = new LongAccumulator(Long::sum, 0L);
    private static final LongAccumulator totalLines = new LongAccumulator(Long::sum, 0);
    private static final LongAccumulator inpReadTime = new LongAccumulator(Long::sum, 0L);
    private static final LongAccumulator readBytes = new LongAccumulator(Long::sum, 0L);

    private static class Result {
        private long count = 0L;
        private double sum = 0.0;
        private double min = Double.MAX_VALUE;
        private double max = Double.MIN_VALUE;

        public void update(double value) {
            ++count;
            sum += value;
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
        }

        public void merge(Result otherResult) {
            count += otherResult.count;
            sum += otherResult.sum;
            min = Math.min(min, otherResult.min);
            max = Math.max(max, otherResult.max);
        }

        public long getCount() {
            return count;
        }

        public double getSum() {
            return sum;
        }

        public double getMin() {
            return min;
        }

        public double getMax() {
            return max;
        }
    }

    private static class Worker implements Runnable {
        private final BlockingQueue<ByteBuffer> fullBuffersQueue;
        private final BlockingQueue<ByteBuffer> freeBuffersQueue;
        private final HashMap<String, Result> result = new HashMap<>();
        public Worker(BlockingQueue<ByteBuffer> inputQueue, BlockingQueue<ByteBuffer> outputQueue) {
            this.fullBuffersQueue = inputQueue;
            this.freeBuffersQueue = outputQueue;
        }

        public Map<String, Result> getResult() {
            return this.result;
        }

        private double readDouble(CharBuffer input, char separator) {
            var output = 0.0;
            var fraction = 0.0;
            boolean isFraction = false;
            while (input.hasRemaining()) {
                var ch = input.get();
                if (ch == separator) {
                    break;
                }
                if (ch == '-') {
                    output = -1.0 * output;
                } else if (ch >= '0' && ch <= '9') {
                    if (isFraction) {
                        fraction = fraction + (ch - '0');
                    } else {
                        output = 10 * output + (ch - '0');
                    }
                } else if (ch == '.') {
                    isFraction = true;
                }
            }
            return output + 0.1 * fraction;
        }
        private String readUntil(CharBuffer input, char separator) {
            var sb = new StringBuilder();
            while (input.hasRemaining()) {
                var ch =  (char) (input.get() & 0xFF);
                if (ch == separator) {
                    break;
                }
                sb.append(ch);
            }
            return sb.toString();
        }

        @Override
        public void run() {
            System.out.println("Worker started");
            final Charset charset = StandardCharsets.UTF_8;
            long lines = 0L;
            try {
                while (true) {
                    ByteBuffer buffer;
                    try {
                        var ts0 = System.currentTimeMillis();
                        buffer = fullBuffersQueue.take();
                        waitCheck.accumulate(System.currentTimeMillis() - ts0);
                    } catch (InterruptedException e) {
                        return;
                    }
                    if (buffer == END_BUF) {
                        break;
                    }
                    lines = 0;
//                    activated.incrementAndGet();

                    var ts1 = System.currentTimeMillis();

                    var charBuffer = charset.decode(buffer);
                    charBuffer.rewind();
                    buffer.position(0);
                    while (buffer.hasRemaining()) {
                        String place = readUntil(charBuffer, ';');
                        double value = readDouble(charBuffer, '\n');
                        ++lines;
                        result.computeIfAbsent(place, (key) -> new Result()).update(value);
                    }

                    var ts2 = System.currentTimeMillis();
                    timeCheck.accumulate(ts2 - ts1);
                    totalLines.accumulate(lines);

                    freeBuffersQueue.add(buffer);
                }
            } catch (Throwable ex) {
                System.err.println("Error at %d - %s".formatted(totalLines.get() + lines, ex.getMessage()));
                freeBuffersQueue.add(END_BUF);
            }
            System.out.println("Worker done.");
        }

        private void mergeResultWith(Map<String, Result> otherResults) {
            otherResults.forEach((key, value) -> {
                result.computeIfAbsent(key, (k) -> new Result()).merge(value);
            });
        }
    }

    private static int reduceEnding(ByteBuffer originalBuffer, byte[] rest) {
        originalBuffer.position(0);
        if (originalBuffer.capacity() <= 1) {
            return 0;
        }
        var pos = originalBuffer.capacity() - 1;
        if (originalBuffer.get(pos) == '\n') {
            return 0;
        }
        while (pos > 0 && originalBuffer.get(pos) != '\n') {
            pos--;
        }
        if (pos <= 0) {
            return 0;
        }
        var copied = originalBuffer.capacity() - pos - 1;
        originalBuffer.get(pos + 1, rest, 0, copied);
        originalBuffer.limit(pos + 1);
        return copied;
    }

    public static void main(String[] args) throws Exception {
        LinkedBlockingDeque<ByteBuffer> fullBuffers = new LinkedBlockingDeque<>();
        LinkedBlockingDeque<ByteBuffer> freeBuffers = new LinkedBlockingDeque<>();
        ExecutorService executors = Executors.newFixedThreadPool(WORKERS);
        List<Worker> workers = IntStream.range(0, WORKERS).mapToObj(i -> new Worker(fullBuffers, freeBuffers)).toList();
        List<? extends Future<?>> futures = workers.stream().map(executors::submit).toList();

        for (int i = 0; i < BUFFERS.length; ++i) {
            BUFFERS[i] = ByteBuffer.allocate(BUFF_SIZE);
            freeBuffers.add(BUFFERS[i]);
        }

        var fileStream = new FileInputStream(FILE);
        var fileChannel = fileStream.getChannel();

        var totalByesCount = 0L;
        var buffCount = 0L;

        var ts0 = System.currentTimeMillis();

        var status = new StatusPrinter(() -> {
            var readBytesPerSec = 1000 * readBytes.get() / timeRead.get();
            var inputBytesPerSec = 1000 * readBytes.get() / timeRead.get();
            var tc = timeCheck.get();
            var checkTimePerWorker = timeCheck.get() / WORKERS;
            var waitPerWorker = waitCheck.get() / WORKERS;
            var waitInMain = waitRead.get();
            var linesProcessPerSec = tc > 0 ? 1000 * totalLines.get() / timeCheck.get() : -1;
            System.out.printf("Time: %d, total lines: %d, read time: %d, read wait: %d, inp read time: %d, check time /w: %d, check wait /w: %d\n",
                    System.currentTimeMillis() - ts0,
                    totalLines.get(),
                    timeRead.get(),
                    waitInMain,
                    inpReadTime.get(),
                    checkTimePerWorker,
                    waitPerWorker
            );
            System.out.println("read MB/s: %d, input MB/s: %d, lines/s: %d".formatted(readBytesPerSec/1_000_000, inputBytesPerSec/1_000_000, linesProcessPerSec));
            System.out.println("bottle neck: %s".formatted(waitInMain > waitPerWorker ? "Workers" : "Reader"));
        });

        byte[] REST_BUFF = new byte[100];
        int restSize = 0;

        while (true) {
            var tsw1 = System.currentTimeMillis();

            var buf = freeBuffers.take();

            waitRead.accumulate(System.currentTimeMillis() - tsw1);
            if (buf == END_BUF) {
                System.err.println("Error. Buffer totalByesCount: %d".formatted(buffCount));
                break;
            }

            var ts1 = System.currentTimeMillis();

            buf.clear();
            ++buffCount;
            if (restSize > 0) {
                buf.put(buf.position(), REST_BUFF, 0, restSize);
                restSize = 0;
            }

            var tsr1 = System.currentTimeMillis();
            var read = fileChannel.read(buf);
            inpReadTime.accumulate(System.currentTimeMillis() - tsr1);

            if (read <= 0) {
                break;
            }

            restSize = reduceEnding(buf, REST_BUFF);
            totalByesCount += read;
            readBytes.accumulate(read);
            buf.rewind();

            var ts2 = System.currentTimeMillis();

            timeRead.accumulate(ts2 - ts1);

            fullBuffers.add(buf);

            status.check();
        }

        for (int w = 0; w < WORKERS; ++w) {
            fullBuffers.add(END_BUF);
        }

        executors.awaitTermination(5, TimeUnit.SECONDS);
        executors.close();

        var ts3 = System.currentTimeMillis();
        for (int w = 1; w < WORKERS; ++w) {
            workers.get(0).mergeResultWith(workers.get(w).getResult());
        }
        var finalResult = workers.get(0).getResult();

        var ts4 = System.currentTimeMillis();
        var keys = finalResult.keySet().stream().sorted();
        var output = keys.map(key -> {
            var result = finalResult.get(key);
            return "%s=%.1f/%.1f/%.1f".formatted(key, result.min, result.sum / result.count, result.max);
        }).collect(Collectors.joining(", "));
        System.out.print("{");
        System.out.print(output);
        System.out.println("}");

        var tsX = System.currentTimeMillis();

        LongSummaryStatistics collect = finalResult.entrySet().stream().collect(Collectors.summarizingLong(r -> r.getValue().count));
        System.out.println("Total actual totalByesCount: %d".formatted(collect.getSum()));

        System.out.println("Count: %s, time: %d".formatted(totalByesCount, tsX - ts0));
        System.out.println("Read time: %d, check time: %d".formatted(timeRead.get(), timeCheck.get()));
        System.out.println("Output time: %d".formatted(tsX - ts4));
    }
}
