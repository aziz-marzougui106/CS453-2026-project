function OpenRead(T, o, dst) -> bool
    now ← CT                                  // sample the clock BEFORE reading o

    // ---- consistent snapshot of the object's metadata ----
    repeat
        s1 ← o.lock, o.ts
        if o.lock is set:  return false       // or bounded spin, your choice
        v, pv, pts ← o.cur, o.prev, o.prev_ts
        s2 ← o.lock, o.ts
    until s1 == s2                            // nothing changed while copying
    ts ← o.ts

    // ---- is the newest version too recent for my window? ----
    if ts > T.max and T.update and T.open:
        Extend(T)                             // may abort internally
        if T aborted: return false

    // ---- case A: newest version fits the window ----
    if ts ≤ T.max:
        T.min ← max(T.min, ts)
        T.max ← min(T.max, now)
        copy v into dst
        T.O ← T.O ∪ {(o, ts)}                 // remember what version I saw
        return true

    // ---- case B: read-only, go back in time ----
    if not T.update and pts ≤ T.max:          // VersionAvailable(o^{T.max})
        newmin ← max(T.min, pts)
        newmax ← min(T.max, ts - 1)           // prev dies just before ts
        if newmin > newmax: return false      // window empty
        T.open ← false                        // right edge is now fixed
        T.min, T.max ← newmin, newmax
        copy pv into dst
        T.O ← T.O ∪ {(o, pts)}
        return true

    // ---- case C: cannot maintain a consistent snapshot ----
    return false


function tm_read(shared, tx, source, size, target) -> bool
    T ← descriptor(tx)

    for each word address w in [source, source + size) in steps of align:
        dst ← target + (w - source)

        // 1. read-your-own-writes
        if w ∈ T.writeset:
            copy T.writeset[w] into dst
            continue

        // 2. otherwise go through LSA's Open
        o ← metadata_of(w)
        if not OpenRead(T, o, dst):
            Abort(T)              // release locks, drop write set, undo allocs
            return false

    return true