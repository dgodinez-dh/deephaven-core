//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharTimSortKernel and run "./gradlew replicateSortKernelTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sort.timsort;

import io.deephaven.test.types.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelTest.class)
public class TestFloatTimSortKernel extends BaseTestFloatTimSortKernel {
    // I like this output, but for now am leaving these tests off, so we can focus on getting right answers and we can
    // try
    // out JMH for running morally equivalent things.

    // @Test
    // public void floatRandomPerformanceTest() {
    // performanceTest(TestFloatTimSortKernel::generateFloatRandom, FloatSortKernelStuff::new, FloatSortKernelStuff::run,
    // getJavaComparator(), FloatMergeStuff::new, FloatMergeStuff::run);
    // }
    //
    // @Test
    // public void floatRunPerformanceTest() {
    // performanceTest(TestFloatTimSortKernel::generateFloatRuns, FloatSortKernelStuff::new, FloatSortKernelStuff::run,
    // getJavaComparator(), FloatMergeStuff::new, FloatMergeStuff::run);
    // }
    //
    // @Test
    // public void floatRunDescendingPerformanceTest() {
    // performanceTest(TestFloatTimSortKernel::generateDescendingFloatRuns, FloatSortKernelStuff::new,
    // FloatSortKernelStuff::run, getJavaComparator(), FloatMergeStuff::new, FloatMergeStuff::run);
    // }
    //
    // @Test
    // public void floatRunAscendingPerformanceTest() {
    // performanceTest(TestFloatTimSortKernel::generateAscendingFloatRuns, FloatSortKernelStuff::new,
    // FloatSortKernelStuff::run, getJavaComparator(), FloatMergeStuff::new, FloatMergeStuff::run);
    // }
    //
    @Test
    public void floatRandomCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestFloatTimSortKernel::generateFloatRandom, getJavaComparator(),
                    FloatLongSortKernelStuff::new);
        }
    }

    @Test
    public void floatAscendingRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestFloatTimSortKernel::generateAscendingFloatRuns, getJavaComparator(),
                    FloatSortKernelStuff::new);
        }
    }

    @Test
    public void floatDescendingRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestFloatTimSortKernel::generateDescendingFloatRuns, getJavaComparator(),
                    FloatSortKernelStuff::new);
        }
    }

    @Test
    public void floatRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestFloatTimSortKernel::generateFloatRuns, getJavaComparator(),
                    FloatSortKernelStuff::new);
        }
    }

    @Test
    public void floatLongRandomCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestFloatTimSortKernel::generateFloatRandom, getJavaComparator(),
                    FloatLongSortKernelStuff::new);
        }
    }

    @Test
    public void floatLongAscendingRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestFloatTimSortKernel::generateAscendingFloatRuns, getJavaComparator(),
                    FloatLongSortKernelStuff::new);
        }
    }

    @Test
    public void floatLongDescendingRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestFloatTimSortKernel::generateDescendingFloatRuns, getJavaComparator(),
                    FloatLongSortKernelStuff::new);
        }
    }

    @Test
    public void floatLongRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestFloatTimSortKernel::generateFloatRuns, getJavaComparator(),
                    FloatLongSortKernelStuff::new);
        }
    }

    @Test
    public void floatRandomPartitionCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_PARTTITION_CHUNK_SIZE; size *= 2) {
            int partitions = 2;
            while (partitions < (int) Math.sqrt(size)) {
                partitionCorrectnessTest(size, size, partitions, TestFloatTimSortKernel::generateFloatRandom,
                        getJavaComparator(), FloatPartitionKernelStuff::new);
                if (size < 1000) {
                    break;
                }
                partitions *= 3;
            }
        }
    }


    @Test
    public void floatMultiRandomCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            multiCorrectnessTest(size, TestFloatTimSortKernel::generateMultiFloatRandom, getJavaMultiComparator(),
                    FloatMultiSortKernelStuff::new);
        }
    }
}
