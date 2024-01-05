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
import java.nio.CharBuffer;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Math.min;
import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_grzegorzaniol {

    private static final String FILE = "./measurements.txt";
    private static final int CHUNK_LINES = 100_000;
    private static final int BUFF_SIZE = 15 * CHUNK_LINES;
    private static final int WORKERS = 1;
    private static final int BUFFERS_COUNT = WORKERS + 2;
    private static final CharBuffer[] BUFFERS = new CharBuffer[BUFFERS_COUNT];
    private static final CharBuffer END_BUF = CharBuffer.allocate(0);

    private static final LongAccumulator timeRead = new LongAccumulator(Long::sum, 0L);
    private static final LongAccumulator waitRead = new LongAccumulator(Long::sum, 0L);
    private static final LongAccumulator timeCheck = new LongAccumulator(Long::sum, 0L);
    private static final LongAccumulator waitCheck = new LongAccumulator(Long::sum, 0L);
    private static final LongAccumulator totalLines = new LongAccumulator(Long::sum, 0);
    private static final LongAccumulator inpReadTime = new LongAccumulator(Long::sum, 0L);
    private static final LongAccumulator readBytes = new LongAccumulator(Long::sum, 0L);

    private static class StatusPrinter {
        private final Runnable r;
        private long ts = System.currentTimeMillis();
        public StatusPrinter(Runnable r) {
            this.r = r;
        }
        public void check() {
            long ts2 = System.currentTimeMillis();
            if (ts2 - ts > 2_000) {
                ts = ts2;
                r.run();
            }
        }
        public void run() {
            r.run();
        }
    }

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
        private final BlockingQueue<CharBuffer> fullBuffersQueue;
        private final BlockingQueue<CharBuffer> freeBuffersQueue;
        private final HashMap<String, Result> result = new HashMap<>();
        public Worker(BlockingQueue<CharBuffer> inputQueue, BlockingQueue<CharBuffer> outputQueue) {
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
                        fraction = 10 * fraction + (ch - '0');
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
                var ch = input.get();
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
            long lines = 0L;
            try {
                while (true) {
                    CharBuffer buffer;
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
                    buffer.position(0);
                    while (buffer.hasRemaining()) {
                        String place = readUntil(buffer, ';');
                        double value = readDouble(buffer, '\n');
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

    private static String reduceEnding(CharBuffer charBuffer) {
        charBuffer.position(0);
        if (charBuffer.length() <= 1) {
            return "";
        }
        var pos = charBuffer.length() - 1;
        if (charBuffer.charAt(pos) == '\n') {
            return "";
        }
        while (pos > 0 && charBuffer.charAt(pos) != '\n') {
            pos--;
        }
        if (pos <= 0) {
            return "";
        }
        var result = charBuffer.subSequence(pos + 1, charBuffer.length()).toString();
        charBuffer.limit(pos + 1);
        return result;
    }

    public static void main(String[] args) throws Exception {
        LinkedBlockingDeque<CharBuffer> fullBuffers = new LinkedBlockingDeque<>();
        LinkedBlockingDeque<CharBuffer> freeBuffers = new LinkedBlockingDeque<>();
        ExecutorService executors = Executors.newFixedThreadPool(WORKERS);
        List<Worker> workers = IntStream.range(0, WORKERS).mapToObj(i -> new Worker(fullBuffers, freeBuffers)).toList();
        List<? extends Future<?>> futures = workers.stream().map(executors::submit).toList();

        for (int i = 0; i < BUFFERS.length; ++i) {
            BUFFERS[i] = CharBuffer.allocate(BUFF_SIZE);
            freeBuffers.add(BUFFERS[i]);
        }

        var x = new FileInputStream(FILE);
        var y = new BufferedInputStream(x, CHUNK_LINES * 15);
        var z = new InputStreamReader(y);

        var count = 0L;
        var buffCount = 0L;

        var ts0 = System.currentTimeMillis();

        var status = new StatusPrinter(() -> {
            var readBytesPerSec = 1000 * readBytes.get() / timeRead.get();
            var inputBytesPerSec = 1000 * readBytes.get() / timeRead.get();
            var linesProcessPerSec = 1000 * totalLines.get() / timeCheck.get();
            System.out.printf("Time: %d, total lines: %d, read time: %d, read wait: %d, inp read time: %d, check time: %d, check wait: %d\n",
                    System.currentTimeMillis() - ts0,
                    totalLines.get(),
                    timeRead.get(),
                    waitRead.get(),
                    inpReadTime.get(),
                    timeCheck.get(),
                    waitCheck.get()
            );
            System.out.println("read bytes/s: %d, input bytes/s: %d, lines/s: %d".formatted(readBytesPerSec, inputBytesPerSec, linesProcessPerSec));
        });

        String ending = null;
        while (true) {
            var tsw1 = System.currentTimeMillis();

            var buf = freeBuffers.take();

            waitRead.accumulate(System.currentTimeMillis() - tsw1);
            if (buf == END_BUF) {
                System.err.println("Error. Buffer count: %d".formatted(buffCount));
                break;
            }

            var ts1 = System.currentTimeMillis();

            buf.clear();
            ++buffCount;
            if (ending != null && !ending.isEmpty()) {
                buf.put(ending);
            }

            var tsr1 = System.currentTimeMillis();
            var read = z.read(buf);
            inpReadTime.accumulate(System.currentTimeMillis() - tsr1);

            if (read <= 0) {
                break;
            }

            ending = reduceEnding(buf);
            count += read;
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
        System.out.println("Total actual count: %d".formatted(collect.getSum()));

        System.out.println("Count: %s, time: %d".formatted(count, tsX - ts0));
        System.out.println("Read time: %d, check time: %d".formatted(timeRead.get(), timeCheck.get()));
        System.out.println("Output time: %d".formatted(tsX - ts4));
    }
}
