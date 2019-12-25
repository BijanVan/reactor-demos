package lite.rx.demo;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

class TopNFluxTest {
    private <T extends Comparable<? super T>> Flux<Stream<T>> topN(Flux<? extends T> stream, int n) {
        SortedSet<T> ss = new TreeSet<>(Comparator.reverseOrder());

        return stream.scan(ss, (acc, e) -> {
            if (acc.size() < n) acc.add(e);
            else {
                T top = acc.last();
                if (e.compareTo(top) > 0) {
                    acc.remove(top);
                    acc.add(e);
                }
            }
            return acc;
        }).map(Collection::stream);
    }

    @DisplayName("Top 5 integers from a random stream.")
    @Test
    void top5IntegerRandom() {
        Random rnd = new Random(System.currentTimeMillis());
        List<Integer> data = Stream.generate(rnd::nextInt).limit(20).collect(Collectors.toList());
        Flux<Integer> stream = Flux.fromIterable(data);
        List<Integer> actual = topN(stream, 5).blockLast().collect(Collectors.toList());
        data.sort(Comparator.reverseOrder());
        List<Integer> expected = data.stream().limit(5).collect(Collectors.toList());
        assertIterableEquals(expected, actual);
    }

    @DisplayName("Top 5 integers from a random stream. StepVerifier")
    @Test
    void top5IntegerRandomVerifySteps() {
        Random rnd = new Random(System.currentTimeMillis());
        List<Integer> data = Stream.generate(rnd::nextInt).limit(8).collect(Collectors.toList());
        StepVerifier.create(topN(Flux.fromIterable(data), 3))
                .expectSubscription()
                .expectNextMatches(s -> s.count() == 0)
                .expectNextMatches(s -> s.count() == 1)
                .expectNextMatches(s -> s.count() == 2)
                .expectNextMatches(s -> s.count() == 3)
                .assertNext(s -> assertTopN(data, s.collect(Collectors.toList()), 4))
                .assertNext(s -> assertTopN(data, s.collect(Collectors.toList()), 5))
                .assertNext(s -> assertTopN(data, s.collect(Collectors.toList()), 6))
                .assertNext(s -> assertTopN(data, s.collect(Collectors.toList()), 7))
                .assertNext(s -> assertTopN(data, s.collect(Collectors.toList()), 8))
//                .thenCancel()
                .verifyComplete();


    }

    private void assertTopN(List<Integer> data, List<Integer> actual, int length) {
        List<Integer> expected = data.subList(0, length);
        expected.sort(Comparator.reverseOrder());
        expected = expected.subList(0, 3);
        assertIterableEquals(expected, actual);
    }
}
