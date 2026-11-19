#include <tm.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <string.h>

// ---- address encoding (unchanged) ----
#define OFFSET_BITS  48
#define OFFSET_MASK  ((((uintptr_t)1) << OFFSET_BITS) - 1)
#define MAX_SEGMENTS (1u << 16)           // ids 1..65535, id 0 never valid

// ---- control word layout (one 64-bit atomic per word) ----
//  bit 0        readable copy: 0 = A, 1 = B     (persistent across epochs)
//  bit 1        written in the epoch tagged below
//  bits 2..33   epoch tag (32 bits)
//  bits 34..63  accessor: 0 = none, MULTI = 2+ transactions, else a tx id
typedef uint64_t ctrl_t;
#define READABLE_BIT ((ctrl_t)1)
#define WRITTEN_BIT  ((ctrl_t)2)
#define TAG_SHIFT    2
#define ACC_SHIFT    34
#define MULTI        ((uint64_t)((1ull << 30) - 1))   // reserved accessor value

// One word in memory:  [ ctrl (8B, padded to word_stride's header) ][ copy A ][ copy B ]
// header = max(align, 8), stride = header + 2*align (all computed once in tm_create)

typedef struct {
    char*  words;              // calloc'd array: nwords * stride bytes (all zero = valid)
    size_t nwords;
} segment;

typedef struct {
    ctrl_t** words;            // control words I wrote (to flip or release later)
    size_t   count, cap;
} word_list;

typedef struct {
    uint32_t* ids;             // segment ids
    size_t    count, cap;
} id_list;

struct shared_region {
    size_t align, size;        // from tm_create
    size_t header, stride;     // per-word layout, derived from align

    // segment table: lazily zeroed, index = segment id
    _Atomic(segment*) segs[MAX_SEGMENTS];
    id_list           free_ids;          // recycled ids (protected by lock)
    uint32_t          next_seg_id;       // next never-used segment id (protected by lock)

    // batcher
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    uint32_t        epoch;               // starts at 1, bumped at epoch end
    size_t          remaining;           // transactions currently in the batch
    size_t          blocked;             // threads waiting for the next batch
    uint32_t        next_tx_id;          // reset to 1 at each epoch start

    // work deferred to the end of the epoch (committed transactions only)
    word_list pending_written;
    id_list   pending_free;
};

struct transaction {
    uint32_t  id;              // unique in this epoch, never 0 or MULTI
    uint32_t  epoch;           // epoch at tm_begin
    bool      is_ro;
    word_list written;         // words this tx made "written"
    id_list   alloc_set;       // segments created by this tx (destroy on abort)
    id_list   free_set;        // segments to release if committed
};
/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align){
    // align must be a power of two, size a positive multiple of it
    if (align == 0 || (align & (align - 1)) != 0) return invalid_shared;
    if (size == 0 || size % align != 0)           return invalid_shared;

    struct shared_region* shr = calloc(1,sizeof(struct shared_region));
    if(!shr) return invalid_shared;
    shr->align=align;shr->size=size;
    shr->header = align > 8 ? align : 8;       // control word padded to keep copies aligned
    shr->stride = shr->header + 2 * align;     // [ctrl][copy A][copy B]
    // (2) the mutex and condvar need real initialisation
    if (pthread_mutex_init(&shr->lock, NULL) != 0) goto fail_region;
    if (pthread_cond_init(&shr->cond, NULL) != 0)  goto fail_mutex;
    shr->free_ids = (id_list){calloc(initial_capcity,sizeof(uint32_t)),0,initial_capcity};
    if(! shr->free_ids.ids) goto fail_cond;
    shr->epoch=1;shr->next_tx_id=1;shr->next_seg_id = 2;

    // (4) the first segment, which your version never created
    size_t nwords = size / align;
    if (nwords > SIZE_MAX / shr->stride) goto fail_ids;     // overflow guard
    segment* seg = malloc(sizeof(segment));
    if (!seg) goto fail_ids;
    seg->words = calloc(nwords, shr->stride);               // all zero = valid initial state
    if (!seg->words) { free(seg); goto fail_ids; }
    seg->nwords = nwords;
    atomic_store(&shr->segs[1], seg);

    return shr;

    fail_ids:free(shr->free_ids.ids);
    fail_cond: free(&(shr->cond));
    fail_mutex:free(&(shr->lock));
    fail_region:free(shr);
    return invalid_shared;
}
/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared){
    struct shared_region* shr=(struct shared_region*)shared;
    for (uint32_t id=1;id<shr->next_seg_id;id++){
        segment* seg=atomic_load(&shr->segs[id]);
        if (seg!=NULL){free(seg->words); free(seg);}
    }
    free(shr->free_ids.ids);
    free(shr->pending_written.words);
    free(shr->pending_free.ids);

    pthread_cond_destroy(&shr->cond);
    pthread_mutex_destroy(&shr->lock);
    free(shr);

}
/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared){
    //(void)shared;
    return (void*)((uintptr_t)1 << OFFSET_BITS);
}
size_t tm_size(shared_t shared){
    if(!shared) return NULL;
    return ((struct shared_region*)shared)->size;
}
size_t tm_align(shared_t shared ){
    if(!shared) return NULL;
    return ((struct shared_region*)shared)->align;
}
/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared, bool is_ro) {
    struct shared_region* shr = (struct shared_region*)shared;

    struct transaction* trx = calloc(1, sizeof(struct transaction));
    if (!trx) return invalid_tx;
    trx->is_ro = is_ro;

    // Read-only transactions never write, alloc or free: skip the lists.
    if (!is_ro) {
        trx->alloc_set = (id_list){calloc(initial_capcity, sizeof(uint32_t)), 0, initial_capcity};
        if (!trx->alloc_set.ids) goto fail_trx;
        trx->free_set = (id_list){calloc(initial_capcity, sizeof(uint32_t)), 0, initial_capcity};
        if (!trx->free_set.ids) goto fail_alloc;
        trx->written = (word_list){calloc(initial_capcity, sizeof(ctrl_t*)), 0, initial_capcity};
        if (!trx->written.words) goto fail_free;
    }

    // Batcher enter (handout pseudocode), under the lock
    pthread_mutex_lock(&shr->lock);
    if (shr->remaining == 0) {
        shr->remaining = 1;
    } else {
        shr->blocked++;
        uint32_t my_epoch = shr->epoch;//epoch of prev. batch
        while (shr->epoch == my_epoch)          // wait for the next batch
            pthread_cond_wait(&shr->cond, &shr->lock);
    }
    trx->epoch = shr->epoch;
    trx->id    = shr->next_tx_id++;             // plain ++, we hold the lock
    pthread_mutex_unlock(&shr->lock);

    return (tx_t)trx;

fail_free:  free(trx->free_set.ids);
fail_alloc: free(trx->alloc_set.ids);
fail_trx:   free(trx);
    return invalid_tx;
}
// ---- small helpers (append with growth) ----
static bool words_append(word_list* dst, ctrl_t** src, size_t n) {
    if (dst->count + n > dst->cap) {
        size_t ncap = dst->cap ? dst->cap : initial_capcity;
        while (ncap < dst->count + n) ncap *= 2;
        ctrl_t** p = realloc(dst->words, ncap * sizeof(ctrl_t*));
        if (!p) return false;
        dst->words = p; dst->cap = ncap;
    }
    memcpy(dst->words + dst->count, src, n * sizeof(ctrl_t*));
    dst->count += n;
    return true;
}
static bool ids_append(id_list* dst, uint32_t const* src, size_t n) {
    if (dst->count + n > dst->cap) {
        size_t ncap = dst->cap ? dst->cap : initial_capcity;
        while (ncap < dst->count + n) ncap *= 2;
        uint32_t* p = realloc(dst->ids, ncap * sizeof(uint32_t));
        if (!p) return false;
        dst->ids = p; dst->cap = ncap;
    }
    memcpy(dst->ids + dst->count, src, n * sizeof(uint32_t));
    dst->count += n;
    return true;
}

// ---- epoch end: caller holds shr->lock and is the last one of the batch ----
static void apply_epoch_end(struct shared_region* shr) {
    // 1. publish: flip readable bit, clear written bit (flips BEFORE frees)
    for (size_t i = 0; i < shr->pending_written.count; i++) {
        ctrl_t* p = shr->pending_written.words[i];
        ctrl_t c = *p;                       // nobody else is running: plain access
        assert(c & WRITTEN_BIT);             // catches duplicates
        c ^= READABLE_BIT;
        c &= ~WRITTEN_BIT;
        *p = c;
    }
    shr->pending_written.count = 0;

    // 2. release freed segments and recycle their ids
    for (size_t i = 0; i < shr->pending_free.count; i++) {
        uint32_t id = shr->pending_free.ids[i];
        segment* seg = atomic_load(&shr->segs[id]);
        free(seg->words);
        free(seg);
        atomic_store(&shr->segs[id], NULL);
        ids_append(&shr->free_ids, &id, 1);  // if this fails the id is just lost
    }
    shr->pending_free.count = 0;

    // 3. next epoch
    shr->epoch++;
    shr->next_tx_id = 1;
}

// ---- leave: used by tm_end AND by every abort path ----
static void leave_batch(struct shared_region* shr) {
    shr->remaining--;
    if (shr->remaining == 0) {
        apply_epoch_end(shr);
        shr->remaining = shr->blocked;
        shr->blocked = 0;
        pthread_cond_broadcast(&shr->cond);
    }
}
/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx){
    struct transaction* trx = (struct transaction*)tx;
    struct shared_region* shr = (struct shared_region*)shared;

    pthread_mutex_lock(&shr->lock);
    if (!trx->is_ro) {
        // hand over committed work BEFORE decrementing, in the same lock hold
        words_append(&shr->pending_written, trx->written.words, trx->written.count);
        ids_append(&shr->pending_free, trx->free_set.ids, trx->free_set.count);
        // alloc_set is NOT undone: those segments are now permanent
    }
    leave_batch(shr);
    pthread_mutex_unlock(&shr->lock);

    free(trx->written.words);
    free(trx->alloc_set.ids);
    free(trx->free_set.ids);
    free(trx);
    return true;
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
    struct shared_region* shr = (struct shared_region*)shared;
    struct transaction* trx = (struct transaction*)tx;
    size_t max_step = size / shr->align;

    // decode ONCE (source is a name, never dereference it)
    uintptr_t addr = (uintptr_t)source;
    size_t seg_id  = addr >> OFFSET_BITS;
    size_t first   = (addr & OFFSET_MASK) / shr->align;
    segment* seg   = atomic_load(&shr->segs[seg_id]);

    char* copy_target = target;
    for (size_t i = 0; i < max_step; i++) {
        char* word = seg->words + (first + i) * shr->stride;
        _Atomic ctrl_t* ctrl = (_Atomic ctrl_t*)word;
        char* copyA = word + shr->header;
        char* copyB = copyA + shr->align;
        char* to_read;

        if (trx->is_ro) {
            ctrl_t h = atomic_load(ctrl);
            to_read = (h & READABLE_BIT) ? copyB : copyA;
        } else {
            ctrl_t h = atomic_load(ctrl);
            while (true) {
                bool fresh = ((h >> TAG_SHIFT) & 0xFFFFFFFFu) != trx->epoch;
                uint64_t acc = fresh?0:(h >> ACC_SHIFT);

                if (!fresh && (h & WRITTEN_BIT)) {
                    if (acc != trx->id) { tx_abort(shr, trx); return false; }
                    // my own write: the writable copy
                    to_read = (h & READABLE_BIT) ? copyA : copyB;
                    break;
                }
                // not written: read the committed copy, register as accessor
                uint64_t newacc = (acc == 0) ? trx->id
                                :(acc == trx->id || acc == MULTI)?acc:MULTI;
                if (!fresh && newacc == acc) {           // nothing to update
                    to_read = (h & READABLE_BIT) ? copyB : copyA;
                    break;
                }
                ctrl_t n = (h & READABLE_BIT)
                         | ((ctrl_t)trx->epoch << TAG_SHIFT)
                         | ((ctrl_t)newacc << ACC_SHIFT);
                if (atomic_compare_exchange_weak(ctrl, &h, n)) {   // h reloaded on failure
                    to_read = (n & READABLE_BIT) ? copyB : copyA;
                    break;
                }
            }
        }
        memcpy(copy_target,to_read,shr->align);
        copy_target+=shr->align;
    }
    return true;
}

void tx_abort(shared_t shared,tx_t tx){
    struct shared_region* shr = (struct shared_region*)shared;
    struct transaction* trx = (struct transaction*)tx;
    for (size_t i=0;i<trx->written.count;++i) {
        _Atomic ctrl_t* ctrl = (_Atomic ctrl_t*)trx->written.words[i];
        ctrl_t h = atomic_load(ctrl);
        atomic_store(ctrl, h & READABLE_BIT);     // untouched: tag 0, no acc, not written . epoch is squashed intentionally as leave batch is about to be called
    }
    for (size_t i = 0; i < trx->alloc_set.count; ++i) {
        uint32_t id = trx->alloc_set.ids[i];
        segment* seg = atomic_load(&shr->segs[id]);
        free(seg->words);
        free(seg);
        atomic_store(&shr->segs[id], NULL);
    }

    // 3. free_set is simply dropped (freed with the lists below)

    pthread_mutex_lock(&shr->lock);
    // give the ids back (under the lock, as free_ids is protected by it)
    ids_append(&shr->free_ids, trx->alloc_set.ids, trx->alloc_set.count);
    leave_batch(shr);
    pthread_mutex_unlock(&shr->lock);

    free(trx->written.words);
    free(trx->alloc_set.ids);
    free(trx->free_set.ids);
    free(trx);
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
    struct shared_region* shr = (struct shared_region*)shared;
    struct transaction* trx = (struct transaction*)tx;

    // decode ONCE (target is a name, never dereference it)
    uintptr_t addr = (uintptr_t)target;
    size_t seg_id  = addr >> OFFSET_BITS;
    size_t first   = (addr & OFFSET_MASK) / shr->align;
    segment* seg   = atomic_load(&shr->segs[seg_id]);

    char const* src = source;
    size_t max_step = size / shr->align;

    for (size_t i = 0; i < max_step; ++i) {
        char* word = seg->words + (first + i) * shr->stride;
        _Atomic ctrl_t* ctrl = (_Atomic ctrl_t*)word;
        char* copyA = word + shr->header;
        char* copyB = copyA + shr->align;
        char* to_write;                       // destination: the writable copy

        ctrl_t h = atomic_load(ctrl);
        while (true) {
            bool     fresh = ((h >> TAG_SHIFT) & 0xFFFFFFFFu) != trx->epoch;
            uint64_t acc   = fresh ? 0 : (h >> ACC_SHIFT);
            bool     written = !fresh && (h & WRITTEN_BIT);

            if (written) {
                if (acc != trx->id) { tx_abort(shr, trx); return false; }
                to_write = (h & READABLE_BIT) ? copyA : copyB;   // my own write
                break;
            }
            if (acc != 0 && acc != trx->id) {                    // someone else accessed it
                tx_abort(shr, trx); return false;
            }
            ctrl_t n = (h & READABLE_BIT)
                     | ((ctrl_t)trx->epoch << TAG_SHIFT)
                     | ((ctrl_t)trx->id << ACC_SHIFT)
                     | WRITTEN_BIT;
            if (atomic_compare_exchange_weak(ctrl, &h, n)) {     // h reloaded on failure
                to_write = (n & READABLE_BIT) ? copyA : copyB;
                // push only after the CAS that made the word written
                if (!words_append(&trx->written, (ctrl_t**)&ctrl, 1)) {
                    tx_abort(shr, trx); return false;
                }
                break;
            }
        }
        memcpy(to_write, src, shr->align);
        src += shr->align;
    }
    return true;
}
/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t shared, tx_t tx, size_t size, void** target) {
    struct shared_region* shr = (struct shared_region*)shared;
    struct transaction* trx = (struct transaction*)tx;
    size_t nb_words = size / shr->align;

    // 1. take an id, under the lock
    uint32_t my_id;
    pthread_mutex_lock(&shr->lock);
    if (shr->free_ids.count > 0) {
        my_id = shr->free_ids.ids[--shr->free_ids.count];
    } else if (shr->next_seg_id < MAX_SEGMENTS) {
        my_id = shr->next_seg_id++;
    } else {
        pthread_mutex_unlock(&shr->lock);
        tx_abort(shr, trx);                 // frees trx, never touch it afterwards
        return abort_alloc;
    }
    pthread_mutex_unlock(&shr->lock);

    // 2. build the segment (zeroed memory = valid initial state)
    segment* sg = malloc(sizeof(segment));
    if (!sg) goto give_back;
    if (nb_words > SIZE_MAX / shr->stride) { free(sg); goto give_back; }
    sg->words = calloc(nb_words, shr->stride);
    if (!sg->words) { free(sg); goto give_back; }
    sg->nwords = nb_words;

    // 3. remember it for abort; on failure undo everything
    if (!ids_append(&trx->alloc_set, &my_id, 1)) {
        free(sg->words); free(sg);
        goto give_back;
    }

    // 4. publish and return the encoded name
    atomic_store(&shr->segs[my_id], sg);
    *target = (void*)((uintptr_t)my_id << OFFSET_BITS);
    return success_alloc;

    give_back:
    pthread_mutex_lock(&shr->lock);
    ids_append(&shr->free_ids, &my_id, 1);
    pthread_mutex_unlock(&shr->lock);
    return nomem_alloc;
}
/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx, void*target){
    struct shared_region* shr = (struct shared_region*)shared;
    struct transaction* trx = (struct transaction*)tx;
    uint32_t seg_id = (uint32_t)((uintptr_t)target >> OFFSET_BITS);   // offset part is 0
    if (!ids_append(&trx->free_set, &seg_id, 1)) {
        tx_abort(shr, trx);
        return false;
    }
    return true;
}