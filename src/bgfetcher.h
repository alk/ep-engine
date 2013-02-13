/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef SRC_BGFETCHER_H_
#define SRC_BGFETCHER_H_ 1

#include "config.h"

#include <list>
#include <map>
#include <string>
#include <vector>

#include "common.h"
#include "dispatcher.h"
#include "item.h"

const uint16_t MAX_BGFETCH_RETRY=5;

class VBucketBGFetchItem {
public:
    VBucketBGFetchItem(const std::string &k, uint64_t s, const void *c) :
                       key(k), cookie(c), retryCount(0), initTime(gethrtime()) {
        value.setId(s);
    }
    ~VBucketBGFetchItem() {}

    void delValue() {
        delete value.getValue();
        value.setValue(NULL);
    }
    bool canRetry() {
        return retryCount < MAX_BGFETCH_RETRY;
    }
    void incrRetryCount() {
        ++retryCount;
    }
    uint16_t getRetryCount() {
        return retryCount;
    }

    const std::string key;
    const void * cookie;
    GetValue value;
    uint16_t retryCount;
    hrtime_t initTime;
};

typedef unordered_map<uint64_t, std::list<VBucketBGFetchItem *> > vb_bgfetch_queue_t;

// Forward declaration.
class EventuallyPersistentStore;
class BgFetcher;

/**
 * A DispatcherCallback for BgFetcher
 */
class BgFetcherCallback : public DispatcherCallback {
public:
    BgFetcherCallback(BgFetcher *b) : bgfetcher(b) { }

    bool callback(Dispatcher &d, TaskId &t);
    std::string description() {
        return std::string("Batching background fetch.");
    }

private:
    BgFetcher *bgfetcher;
};

/**
 * Dispatcher job responsible for batching data reads and push to
 * underlying storage
 */
class BgFetcher {
public:
    static const double sleepInterval;
    /**
     * Construct a BgFetcher task.
     *
     * @param s the store
     * @param d the dispatcher
     */
    BgFetcher(EventuallyPersistentStore *s, Dispatcher *d, EPStats &st) :
        store(s), dispatcher(d), stats(st) {}

    void start(void);
    void stop(void);
    bool run(TaskId &tid);
    bool pendingJob(void);

    void notifyBGEvent(void) {
        if (++stats.numRemainingBgJobs == 1) {
            LockHolder lh(taskMutex);
            assert(task.get());
            dispatcher->wake(task);
        }
    }

private:
    void doFetch(uint16_t vbId);
    void clearItems(uint16_t vbId);

    EventuallyPersistentStore *store;
    Dispatcher *dispatcher;
    vb_bgfetch_queue_t items2fetch;
    size_t total_num_fetched_items;
    size_t total_num_requeued_items;
    TaskId task;
    Mutex taskMutex;
    EPStats &stats;
};

#endif  // SRC_BGFETCHER_H_