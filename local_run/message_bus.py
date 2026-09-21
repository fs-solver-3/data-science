"""In-process stand-in for the Kafka topic.

A bounded queue carrying the same JSON-encoded bytes the real producer sends,
so producer/consumer code is unchanged in shape: the producer serialises to
UTF-8 JSON, the consumer decodes and json.loads it.

Bounded (rather than unbounded) so a fast producer applies backpressure
instead of buffering all 527k messages in memory, which is roughly what
Kafka's own flow control does for you.
"""
import queue

# Sentinel pushed by the producer to signal end-of-stream. Kafka has no
# equivalent -- the real consumer polls forever -- so this is what lets the
# local run terminate.
EOS = object()


class MessageBus:
    def __init__(self, maxsize=10000):
        self._q = queue.Queue(maxsize=maxsize)
        self.produced = 0
        self.consumed = 0

    def produce(self, value: bytes):
        self._q.put(value)
        self.produced += 1

    def close(self):
        """Signal end of stream to the consumer."""
        self._q.put(EOS)

    def poll(self, timeout=1.0):
        """Return the next message, EOS at end of stream, or None on timeout.

        Mirrors confluent_kafka's Consumer.poll(timeout) contract, which
        returns None when no message arrived in the window.
        """
        try:
            msg = self._q.get(timeout=timeout)
        except queue.Empty:
            return None
        if msg is not EOS:
            self.consumed += 1
        return msg
