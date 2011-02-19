/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef QUEUEDITEM_HH
#define QUEUEDITEM_HH 1

#include "common.hh"
#include "item.hh"
#include "stats.hh"

enum queue_operation {
    queue_op_set,
    queue_op_del,
    queue_op_flush,
    queue_op_empty,
    queue_op_commit
};

typedef enum {
    vbucket_del_success,
    vbucket_del_fail,
    vbucket_del_invalid
} vbucket_del_result;

/**
 * Representation of an item queued for persistence or tap.
 */
class QueuedItem {
public:
    QueuedItem(const std::string &k, const uint16_t vb, enum queue_operation o,
               const uint16_t vb_version = -1, const int64_t rid = -1, const uint32_t f = 0,
               const time_t expiry_time = 0, const uint64_t cv = 0)
        : op(o),vbucket_version(vb_version), queued(ep_current_time()),
          dirtied(ep_current_time()), item(new Item(k, f, expiry_time, NULL, 0, cv, rid, vb)) { }

    QueuedItem(const std::string &k, value_t v, const uint16_t vb, enum queue_operation o,
               const uint16_t vb_version = -1, const int64_t rid = -1, const uint32_t f = 0,
               const time_t expiry_time = 0, const uint64_t cv = 0)
        : op(o), vbucket_version(vb_version), queued(ep_current_time()),
          dirtied(ep_current_time()), item(new Item(k, f, expiry_time, v, cv, rid, vb)) { }

    ~QueuedItem() { }

    const std::string &getKey(void) const { return item->getKey(); }
    uint16_t getVBucketId(void) const { return item->getVBucketId(); }
    uint16_t getVBucketVersion(void) const { return vbucket_version; }
    uint32_t getQueuedTime(void) const { return queued; }
    enum queue_operation getOperation(void) const { return op; }
    int64_t getRowId() const { return item->getId(); }
    uint32_t getDirtiedTime() const { return dirtied; }
    uint32_t getFlags() const { return item->getFlags(); }
    time_t getExpiryTime() const { return item->getExptime(); }
    uint64_t getCas() const { return item->getCas(); }
    value_t getValue() const { return item->getValue(); }
    Item &getItem() { return *item; }

    void setQueuedTime(uint32_t queued_time) {
        queued = queued_time;
    }

    bool operator <(const QueuedItem &other) const {
        return getVBucketId() == other.getVBucketId() ?
            getKey() < other.getKey() : getVBucketId() < other.getVBucketId();
    }

    size_t size() const {
        return sizeof(QueuedItem) + item->size();
    }

    static void init(EPStats * st) {
        stats = st;
    }

private:
    enum queue_operation op;
    uint16_t vbucket_version;
    uint32_t queued;

    // Additional variables below are required to support the checkpoint and cursors
    // as memory hashtable always contains the latest value and latest meta data for each key.
    uint32_t dirtied;
    shared_ptr<Item> item;

    static EPStats *stats;
};

typedef shared_ptr<QueuedItem> queued_item;

/**
 * Order QueuedItem objects pointed by shared_ptr by their keys.
 */
class CompareQueuedItemsByKey {
public:
    CompareQueuedItemsByKey() {}
    bool operator()(const queued_item &i1, const queued_item &i2) {
        return i1->getKey() < i2->getKey();
    }
};

/**
 * Order QueuedItem objects by their row ids.
 */
class CompareQueuedItemsByRowId {
public:
    CompareQueuedItemsByRowId() {}
    bool operator()(const queued_item &i1, const queued_item &i2) {
        return i1->getRowId() < i2->getRowId();
    }
};

/**
 * Order QueuedItem objects by their vbucket then row ids.
 */
class CompareQueuedItemsByVBAndRowId {
public:
    CompareQueuedItemsByVBAndRowId() {}
    bool operator()(const queued_item &i1, const queued_item &i2) {
        return i1->getVBucketId() == i2->getVBucketId()
            ? i1->getRowId() < i2->getRowId()
            : i1->getVBucketId() < i2->getVBucketId();
    }
};

/**
 * Compare two Schwartzian-transformed QueuedItems.
 */
template <typename T>
class TaggedQueuedItemComparator {
public:
    TaggedQueuedItemComparator() {}

    bool operator()(std::pair<T, QueuedItem> i1, std::pair<T, QueuedItem> i2) {
        return (i1.first == i2.first)
            ? (i1.second.getRowId() < i2.second.getRowId())
            : (i1 < i2);
    }
};

#endif /* QUEUEDITEM_HH */
