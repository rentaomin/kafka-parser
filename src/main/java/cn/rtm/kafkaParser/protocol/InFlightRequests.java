package cn.rtm.kafkaParser.protocol;


import org.apache.kafka.common.requests.RequestHeader;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class InFlightRequests {

    private final Map<String, Deque<RequestHeader>> requests = new HashMap<>();

    /** Thread safe total number of in flight requests. */
    private final AtomicInteger RequestHeaderCount = new AtomicInteger(0);

    /**
     * Add the given request to the queue for the connection it was directed to
     */
    public void add(String destination, RequestHeader requestHeader) {
        Deque<RequestHeader> reqs = this.requests.computeIfAbsent(destination, k -> new ArrayDeque<>());
        reqs.addFirst(requestHeader);
        RequestHeaderCount.incrementAndGet();
    }

    /**
     * Get the request queue for the given node
     */
    private Deque<RequestHeader> requestQueue(String node) {
        Deque<RequestHeader> reqs = requests.get(node);
//        if (reqs == null || reqs.isEmpty())
//            throw new IllegalStateException("There are no in-flight requests for node " + node);
        return reqs;
    }

    /**
     * Get the oldest request (the one that will be completed next) for the given node
     */
    public RequestHeader completeNext(String node) {
        Deque<RequestHeader> requestHeaders = requestQueue(node);
        if (requestHeaders == null) {
            return null;
        }
        RequestHeader RequestHeader = requestHeaders.pollLast();
        RequestHeaderCount.decrementAndGet();
        return RequestHeader;
    }

    /**
     * Get the last request we sent to the given node (but don't remove it from the queue)
     * @param node The node id
     */
    public RequestHeader lastSent(String node) {
        return requestQueue(node).peekFirst();
    }

    /**
     * Complete the last request that was sent to a particular node.
     * @param node The node the request was sent to
     * @return The request
     */
    public RequestHeader completeLastSent(String node) {
        RequestHeader RequestHeader = requestQueue(node).pollFirst();
        RequestHeaderCount.decrementAndGet();
        return RequestHeader;
    }

    /**
     * Return the number of in-flight requests directed at the given node
     * @param node The node
     * @return The request count.
     */
    public int count(String node) {
        Deque<RequestHeader> queue = requests.get(node);
        return queue == null ? 0 : queue.size();
    }

    /**
     * Return true if there is no in-flight request directed at the given node and false otherwise
     */
    public boolean isEmpty(String node) {
        Deque<RequestHeader> queue = requests.get(node);
        return queue == null || queue.isEmpty();
    }

    /**
     * Count all in-flight requests for all nodes. This method is thread safe, but may lag the actual count.
     */
    public int count() {
        return RequestHeaderCount.get();
    }

    /**
     * Return true if there is no in-flight request and false otherwise
     */
    public boolean isEmpty() {
        for (Deque<RequestHeader> deque : this.requests.values()) {
            if (!deque.isEmpty())
                return false;
        }
        return true;
    }

    /**
     * Clear out all the in-flight requests for the given node and return them
     *
     * @param node The node
     * @return All the in-flight requests for that node that have been removed
     */
    public Iterable<RequestHeader> clearAll(String node) {
        Deque<RequestHeader> reqs = requests.get(node);
        if (reqs == null) {
            return Collections.emptyList();
        } else {
            final Deque<RequestHeader> clearedRequests = requests.remove(node);
            RequestHeaderCount.getAndAdd(-clearedRequests.size());
            return clearedRequests::descendingIterator;
        }
    }

}
