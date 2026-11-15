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
void*    tm_start(shared_t);
size_t   tm_size(shared_t);
size_t   tm_align(shared_t);
tx_t     tm_begin(shared_t, bool);
bool     tm_end(shared_t, tx_t);
bool     tm_read(shared_t, tx_t, void const*, size_t, void*);
bool     tm_write(shared_t, tx_t, void const*, size_t, void*);
alloc_t  tm_alloc(shared_t, tx_t, size_t, void**);
bool     tm_free(shared_t, tx_t, void*);