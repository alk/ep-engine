/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
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
#include "item.hh"

#include "tools/cJSON.h"

#include "minimalloc.h"
#include <stdio.h>

Atomic<uint64_t> Item::casCounter(1);
const uint32_t Item::metaDataSize(2 * sizeof(uint32_t) + 2 * sizeof(uint64_t) + 2);

Mutex Blob::allocation_mutex;

bool Item::append(const Item &i) {
    assert(value.get() != NULL);
    assert(i.getValue().get() != NULL);
    size_t newSize = value->length() + i.getValue()->length();
    Blob *newData = Blob::New(newSize);
    char *newValue = (char *) newData->getData();
    std::memcpy(newValue, value->getData(), value->length());
    std::memcpy(newValue + value->length(), i.getValue()->getData(), i.getValue()->length());
    value.reset(newData);
    return true;
}

/**
 * Prepend another item to this item
 *
 * @param itm the item to prepend to this one
 * @return true if success
 */
bool Item::prepend(const Item &i) {
    assert(value.get() != NULL);
    assert(i.getValue().get() != NULL);
    size_t newSize = value->length() + i.getValue()->length();
    Blob *newData = Blob::New(newSize);
    char *newValue = (char *) newData->getData();
    std::memcpy(newValue, i.getValue()->getData(), i.getValue()->length());
    std::memcpy(newValue + i.getValue()->length(), value->getData(), value->length());
    value.reset(newData);
    return true;
}

static mini_state *blob_malloc_st;

Blob *Blob::allocate_blob(size_t total_len)
{
    void *rv;
    size_t actual_size;
    EventuallyPersistentEngine *old = ObjectRegistry::onSwitchThread(NULL, true);
    {
        LockHolder lh(allocation_mutex);
        if (!blob_malloc_st) {
            blob_malloc_st = mini_init(malloc, free);
            if (!blob_malloc_st) {
                abort();
            }
        }
        rv = mini_malloc(blob_malloc_st, total_len);
        if (!rv) {
            abort();
        }
        actual_size = mini_usable_size(blob_malloc_st, rv);
    }
    ObjectRegistry::onSwitchThread(old);
    ObjectRegistry::memoryAllocated(actual_size);
    // fprintf(stderr, "Allocated %p of %u\n", rv, total_len);
    return static_cast<Blob *>(rv);
}

void Blob::deallocate_blob(Blob *p)
{
    size_t returned;

    if (!p) {
        return;
    }

    EventuallyPersistentEngine *old = ObjectRegistry::onSwitchThread(NULL, true);
    {
        LockHolder lh(allocation_mutex);
        assert(blob_malloc_st);

        returned = mini_usable_size(blob_malloc_st, p);

        mini_free(blob_malloc_st, p);
    }
    // fprintf(stderr, "Freed %p of %u\n", p, returned);
    ObjectRegistry::onSwitchThread(old);
    ObjectRegistry::memoryDeallocated(returned);
}


extern "C"
void printout_blob_mallinfo(void)
{
    struct mini_stats stats;
    {
        LockHolder lh(Blob::allocation_mutex);
        mini_get_stats(blob_malloc_st, &stats, 0, 0);
    }
    fprintf(stderr, "blob malloc stats:\nos_chunks_count: %u\nfree_spans_count: %u\nfree_space: %llu\n", stats.os_chunks_count, stats.free_spans_count, (unsigned long long)stats.free_space);
}
