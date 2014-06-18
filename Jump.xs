#define PERL_NO_GET_CONTEXT
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#include "const-c.inc"

#include <stdint.h>
#include <stdlib.h>

// FloodyBerry's public domain SipHash implementation
// protect these since they might be defined if we have a recent enough Perl
#ifndef U8TO64_LE
static inline uint64_t U8TO64_LE(const unsigned char *p) { return *(const uint64_t *)p; }
#endif
#ifndef ROTL64
#define ROTL64(a,b) (((a)<<(b))|((a)>>(64-b)))
#endif

static uint64_t
siphash(const unsigned char key[16], const unsigned char *m, size_t len) {
        uint64_t v0, v1, v2, v3;
        uint64_t mi, k0, k1;
        uint64_t last7;
        size_t i, blocks;

        k0 = U8TO64_LE(key + 0);
        k1 = U8TO64_LE(key + 8);
        v0 = k0 ^ 0x736f6d6570736575ull;
        v1 = k1 ^ 0x646f72616e646f6dull;
        v2 = k0 ^ 0x6c7967656e657261ull;
        v3 = k1 ^ 0x7465646279746573ull;

        last7 = (uint64_t)(len & 0xff) << 56;

#define sipcompress() \
        v0 += v1; v2 += v3; \
        v1 = ROTL64(v1,13);     v3 = ROTL64(v3,16); \
        v1 ^= v0; v3 ^= v2; \
        v0 = ROTL64(v0,32); \
        v2 += v1; v0 += v3; \
        v1 = ROTL64(v1,17); v3 = ROTL64(v3,21); \
        v1 ^= v2; v3 ^= v0; \
        v2 = ROTL64(v2,32);

        for (i = 0, blocks = (len & ~7); i < blocks; i += 8) {
                mi = U8TO64_LE(m + i);
                v3 ^= mi;
                sipcompress()
                sipcompress()
                v0 ^= mi;
        }

        switch (len - blocks) {
                case 7: last7 |= (uint64_t)m[i + 6] << 48;
                case 6: last7 |= (uint64_t)m[i + 5] << 40;
                case 5: last7 |= (uint64_t)m[i + 4] << 32;
                case 4: last7 |= (uint64_t)m[i + 3] << 24;
                case 3: last7 |= (uint64_t)m[i + 2] << 16;
                case 2: last7 |= (uint64_t)m[i + 1] <<  8;
                case 1: last7 |= (uint64_t)m[i + 0]      ;
                case 0:
                default:;
        };
        v3 ^= last7;
        sipcompress()
        sipcompress()
        v0 ^= last7;
        v2 ^= 0xff;
        sipcompress()
        sipcompress()
        sipcompress()
        sipcompress()
        return v0 ^ v1 ^ v2 ^ v3;
}

// Google's JumpHash -- http://arxiv.org/abs/1406.2294
static uint32_t jumphash(uint64_t key, int32_t numBuckets) {

	int64_t b = -1, j = 0;

	while (j < numBuckets) {
		b = j;
		key = key*2862933555777941757ULL + 1;
                j = (b + 1) * (double)(1LL << 31) / (double)((key >> 33) + 1);
	}

	return b;
}

MODULE = ShardedKV::Continuum::Jump		PACKAGE = ShardedKV::Continuum::Jump::XS

INCLUDE: const-xs.inc

int
lookup(b, buckets)
        SV * b
        uint32_t buckets
    CODE:
        STRLEN  blen = 0;
        const unsigned char *bptr = SvPVbyte(b, blen);
        unsigned char key[16];
        memset(key, 0, sizeof(key));
        uint64_t h = siphash(key, bptr, blen);
        RETVAL = jumphash(h, buckets);
    OUTPUT:
        RETVAL
