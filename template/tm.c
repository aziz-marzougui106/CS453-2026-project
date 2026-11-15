/**
 * @file   tm.c
 * @author [...]
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of your own transaction manager.
 * You can completely rewrite this file (and create more files) as you wish.
 * Only the interface (i.e. exported symbols and semantic) must be preserved.
**/

// Requested features
#define _GNU_SOURCE
#define _POSIX_C_SOURCE   200809L
#ifdef __STDC_NO_ATOMICS__
    #error Current C11 compiler does not support atomic operations
#endif

// External headers

// Internal headers
#include <tm.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "macros.h"
#include <stdatomic.h>
#include <stdalign.h>
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
// Lock format: High bits = version, Low bit = lock bit
typedef uint64_t lock_t;
#define LOCK_BIT      ((lock_t)1)
#define VER_OF(v)     ((v) >> 1)
#define NO_PREV       UINT64_MAX      // prev_ts sentinel: no previous version
#define MAX_SEGMENTS  (1u << 16)      // address = (segment_id << OFFSET_BITS) | offset
#define OFFSET_BITS   48

typedef struct _segment segment;

// One per word of shared memory
typedef struct {
    _Atomic lock_t   vlock;    // (ts << 1) | lock_bit
    _Atomic uint64_t cur;      // value of newest version (born at ts)
    _Atomic uint64_t prev;     // value of previous version, valid in [prev_ts, ts-1]
    _Atomic uint64_t prev_ts;  // birth time of prev, or NO_PREV
} object_metadata;

struct _segment {
    object_metadata* obj;      // array, one entry per word
    size_t           nwords;
    _Atomic bool     freed;    // set when a tm_free commits
};

struct shared_region {
    alignas(64) _Atomic uint64_t gvc;        // global clock, own cache line
    alignas(64) size_t size;                 // size of first segment (bytes)
    size_t align;
    _Atomic(segment*) segs[MAX_SEGMENTS];    // indexed by segment id; id 0 unused
    _Atomic uint32_t  next_id;               // next free segment id, starts at 1
};

// Read-set entry
typedef struct {
    object_metadata* obj;
    uint64_t         ts;       // version (without lock bit) that was read
} read_entry_t;

// Write-set entry
typedef struct {
    object_metadata* obj;
    uint64_t         value;    // new value, buffered until commit
} write_entry_t;

struct transaction {
    uint64_t min, max;         // snapshot window [min, max]
    bool open;
    bool update;               // LSA flag; never set if is_ro
    bool is_ro;                // fixed at tm_begin

    read_entry_t*  read_set;   size_t read_count,  read_cap;
    write_entry_t* write_set;  size_t write_count, write_cap;

    segment** alloc_set;       size_t alloc_count, alloc_cap;   // undo on abort
    segment** free_set;        size_t free_count,  free_cap;    // apply on commit

    object_metadata** held;    size_t held_count;               // locks taken at commit
};

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) {
    // TODO: tm_create(size_t, size_t)
    if(size%align!=0) return invalid_shared;
    struct shared_region* shr=malloc(sizeof(struct shared_region));
    if(!shr) return invalid_shared;
    memset(shr,0,sizeof(struct shared_region));
    shr->gvc=0;
    shr->size=size;shr->align=align;
    shr->next_id=1;
    //shr->start_addr = aligned_alloc(align,size);if(!shr->start_addr) return invalid_shared;
    return(shared_t)shr;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t unused(shared)) {
    // TODO: tm_destroy(shared_t)

}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) {
    // TODO: tm_start(shared_t)
    if(!shared) return NULL;
    return ((struct shared_region*)shared)->segs[0];
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t unused(shared)) {
    // TODO: tm_size(shared_t)
    if(!shared) return 0;
    return ((struct shared_region*)shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t unused(shared)) {
    // TODO: tm_align(shared_t)
    if(!shared) return 0;
    return ((struct shared_region*)shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared, bool is_ro) {
    struct transaction* trx = malloc(sizeof(tx_t));
    if(!trx) return invalid_tx;
    struct shared_region* shr=(struct shared_region*) shared;
    memset(trx,0,sizeof(struct transaction));
    trx->is_ro=is_ro;
    
    trx->min= shr->gvc;
    trx->read_cap=initial_capcity;
    trx->read_count=(trx->write_count=0);
    trx->read_set=calloc(initial_capcity,sizeof(read_entry_t));
    if(!trx->read_set) return invalid_tx;
    if(!is_ro){
        trx->write_cap=initial_capcity;
        trx->write_set=calloc(initial_capcity,sizeof(write_entry_t));
        if(!trx->write_set) return invalid_tx;
    }
    trx->alloc_set = trx->free_set = NULL;
    return (tx_t*)trx;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx) {
    // TODO: tm_end(shared_t, tx_t)
    return false;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared, tx_t tx, void const* source, size_t size, void* target) {
    // TODO: tm_read(shared_t, tx_t, void const*, size_t, void*)
    if(size==0)return true;
    struct transaction* trx= (struct transaction*) tx;
    struct shared_region* shr = (struct shared_region*) shared;
    size_t current_size=0;
    while(current_size < size){
        void* w= source+current_size;
        current_size+=shr->align;
        if (in_write_set(trx,w)){

        }
    }
    

    return false;
}
bool in_write_set(struct transaction* trx,void const* w){
    return true;
}
/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) {
    // TODO: tm_write(shared_t, tx_t, void const*, size_t, void*)
    struct transaction* trx=(struct transaction*)tx;
    struct shared_region* shr=(struct shared_region*) shared;
    uint64_t now = shr->gvc;
    size_t obj_idx= (uint64_t)target & (1<<OFFSET_BITS -1);
    size_t seg_idx= (uint64_t)target>>OFFSET_BITS;
    size_t nb_objects= size/shr->align;
    object_metadata* obj = (shr->segs[seg_idx]->obj)+obj_idx;

    for(size_t i=0;i<nb_objects;++i){
        ;
    }
    return false;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t unused(shared), tx_t unused(tx), size_t unused(size), void** unused(target)) {
    // TODO: tm_alloc(shared_t, tx_t, size_t, void**)
    return abort_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t unused(shared), tx_t unused(tx), void* unused(target)) {
    // TODO: tm_free(shared_t, tx_t, void*)
    if(1);
    return false;
}
