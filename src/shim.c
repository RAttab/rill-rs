/* shim.c
   Rémi Attab (remi.attab@gmail.com), 14 Sep 2017
   FreeBSD-style copyright and disclaimer apply

   Rust doesn't really handle flexible array members so this shim helps to
   bridge that gap.
*/

#include "rill.h"

#include <assert.h>


// -----------------------------------------------------------------------------
// pairs
// -----------------------------------------------------------------------------

size_t rill_pairs_cap(struct rill_pairs *pairs)
{
    return pairs->cap;
}

size_t rill_pairs_len(struct rill_pairs *pairs)
{
    return pairs->len;
}

struct rill_kv *rill_pairs_get(struct rill_pairs *pairs, size_t i)
{
    assert(i < pairs->len);
    return &pairs->data[i];
}
