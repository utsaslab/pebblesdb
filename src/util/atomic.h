// This is a copy of e/atomic.h from libe
//
// This code is derived from code distributed as part of Google Perftools.
// The original is available in from google-perftools-2.0 in the file
// src/base/atomicops-internals-x86.h.  This file was retrieved Feb 06, 2012 by
// Robert Escriva.

/* Copyright (c) 2006, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * ---
 * Original Author: Sanjay Ghemawat (don't blame him for bugs in this derivative)
 */

#ifndef leveldb_atomic_h_
#define leveldb_atomic_h_

#include <stdint.h>
#ifdef _MSC_VER
#define _WINSOCKAPI_
#include <Windows.h>
#endif

// This struct is not part of the public API of this module; clients may not
// use it.
// Features of this x86.  Values may not be correct before main() is run,
// but are set conservatively.
struct LevelDB_AtomicOps_x86CPUFeatureStruct {
  bool has_amd_lock_mb_bug; // Processor has AMD memory-barrier bug; do lfence
                            // after acquire compare-and-swap.
  bool has_sse2;            // Processor has SSE2.
  bool has_cmpxchg16b;      // Processor supports cmpxchg16b instruction.
};
extern struct LevelDB_AtomicOps_x86CPUFeatureStruct LevelDB_AtomicOps_Internalx86CPUFeatures;

#ifdef _MSC_VER
#define ATOMICOPS_COMPILER_BARRIER() MemoryBarrier()
#else
#define ATOMICOPS_COMPILER_BARRIER() __asm__ __volatile__("" : : : "memory")
#endif


namespace leveldb
{
namespace atomic
{

inline void
memory_barrier()
{
#ifdef _MSC_VER
    MemoryBarrier();
#elif defined(__x86_64__)
    __asm__ __volatile__("mfence" : : : "memory");
#else
    if (LevelDB_AtomicOps_Internalx86CPUFeatures.has_sse2)
    {
        __asm__ __volatile__("mfence" : : : "memory");
    }
    else
    {
        uint32_t x = 0;
        exchange_32_fullbarrier(&x, 0);
    }
#endif
}

///////////////////////////////////// Store ////////////////////////////////////

inline void
store_32_nobarrier(volatile uint32_t* ptr, uint32_t value)
{
    *ptr = value;
}

inline void
store_32_acquire(volatile uint32_t* ptr, uint32_t value)
{
    *ptr = value;
    memory_barrier();
}

inline void
store_32_release(volatile uint32_t* ptr, uint32_t value)
{
    ATOMICOPS_COMPILER_BARRIER();
    *ptr = value; // An x86 store acts as a release barrier.
    // See comments in 64-bit version of Release_Store(), below.
}

inline void
store_64_nobarrier(volatile uint64_t* ptr, uint64_t value)
{
#if defined(__x86_64__)
    *ptr = value;
#else
    __asm__ __volatile__("movq %1, %%mm0\n\t"  // Use mmx reg for 64-bit atomic
                         "movq %%mm0, %0\n\t"  // moves (ptr could be read-only)
                         "emms\n\t"            // Empty mmx state/Reset FP regs
                         : "=m" (*ptr)
                         : "m" (value)
                         : // mark the FP stack and mmx registers as clobbered
                           "st", "st(1)", "st(2)", "st(3)", "st(4)",
                           "st(5)", "st(6)", "st(7)", "mm0", "mm1",
                           "mm2", "mm3", "mm4", "mm5", "mm6", "mm7");
#endif
}

inline void
store_64_acquire(volatile uint64_t* ptr, uint64_t value)
{
    store_64_nobarrier(ptr, value);
    memory_barrier();
}

inline void
store_64_release(volatile uint64_t* ptr, uint64_t value)
{
    ATOMICOPS_COMPILER_BARRIER();
    store_64_nobarrier(ptr, value);
    // An x86 store acts as a release barrier
    // for current AMD/Intel chips as of Jan 2008.
    // See also Acquire_Load(), below.

    // When new chips come out, check:
    //  IA-32 Intel Architecture Software Developer's Manual, Volume 3:
    //  System Programming Guide, Chatper 7: Multiple-processor management,
    //  Section 7.2, Memory Ordering.
    // Last seen at:
    //   http://developer.intel.com/design/pentium4/manuals/index_new.htm
    //
    // x86 stores/loads fail to act as barriers for a few instructions (clflush
    // maskmovdqu maskmovq movntdq movnti movntpd movntps movntq) but these are
    // not generated by the compiler, and are rare.  Users of these instructions
    // need to know about cache behaviour in any case since all of these involve
    // either flushing cache lines or non-temporal cache hints.
}

template <typename P>
inline void
store_ptr_nobarrier(P* volatile* ptr, P* value)
{
    *ptr = value;
}

template <typename P>
inline void
store_ptr_acquire(P* volatile* ptr, P* value)
{
    *ptr = value;
    memory_barrier();
}

template <typename P>
inline void
store_ptr_release(P* volatile* ptr, P* value)
{
    ATOMICOPS_COMPILER_BARRIER();

    *ptr = value; // An x86 store acts as a release barrier
    // for current AMD/Intel chips as of Jan 2008.
    // See also Acquire_Load(), below.
}

template <typename P>
inline void
store_ptr_fullbarrier(P* volatile* ptr, P* value)
{
    ATOMICOPS_COMPILER_BARRIER();
    *ptr = value; // An x86 store acts as a release barrier
    memory_barrier();
}

///////////////////////////////////// Load /////////////////////////////////////

inline uint32_t
load_32_nobarrier(volatile const uint32_t* ptr)
{
    return *ptr;
}

inline uint32_t
load_32_acquire(volatile const uint32_t* ptr)
{
    uint32_t value = *ptr; // An x86 load acts as a acquire barrier.
    // See comments in 64-bit version of Release_Store(), below.
    ATOMICOPS_COMPILER_BARRIER();
    return value;
}

inline uint32_t
load_32_release(volatile const uint32_t* ptr)
{
    memory_barrier();
    return *ptr;
}

inline uint64_t
load_64_nobarrier(volatile const uint64_t* ptr)
{
#if defined(__x86_64__)
    return *ptr;
#else
    uint64_t value;
    __asm__ __volatile__("movq %1, %%mm0\n\t"  // Use mmx reg for 64-bit atomic
                         "movq %%mm0, %0\n\t"  // moves (ptr could be read-only)
                         "emms\n\t"            // Empty mmx state/Reset FP regs
                         : "=m" (value)
                         : "m" (*ptr)
                         : // mark the FP stack and mmx registers as clobbered
                           "st", "st(1)", "st(2)", "st(3)", "st(4)",
                           "st(5)", "st(6)", "st(7)", "mm0", "mm1",
                           "mm2", "mm3", "mm4", "mm5", "mm6", "mm7");
    return value;
#endif
}

inline uint64_t
load_64_acquire(volatile const uint64_t* ptr)
{
    // An x86 load acts as a acquire barrier,
    // for current AMD/Intel chips as of Jan 2008.
    // See also Release_Store(), above.
    uint64_t value = load_64_nobarrier(ptr);
    ATOMICOPS_COMPILER_BARRIER();
    return value;
}

inline uint64_t
load_64_release(volatile const uint64_t* ptr)
{
    memory_barrier();
    return load_64_nobarrier(ptr);
}

template <typename P>
inline P*
load_ptr_nobarrier(P* volatile const* ptr)
{
    return *ptr;
}

template <typename P>
inline P*
load_ptr_acquire(P* volatile const* ptr)
{
    P* value = *ptr; // An x86 load acts as a acquire barrier,
    // for current AMD/Intel chips as of Jan 2008.
    // See also Release_Store(), above.
    ATOMICOPS_COMPILER_BARRIER();
    return value;
}

template <typename P>
inline P*
load_ptr_release(P* volatile const* ptr)
{
    memory_barrier();
    return *ptr;
}

/////////////////////////////// Compare and Swap ///////////////////////////////

#if !defined(__x86_64__)
#if !((__GNUC__ > 4) || (__GNUC__ == 4 && __GNUC_MINOR__ >= 1))
// For compilers older than gcc 4.1, we use inline asm.
//
// Potential pitfalls:
//
// 1. %ebx points to Global offset table (GOT) with -fPIC.
//    We need to preserve this register.
// 2. When explicit registers are used in inline asm, the
//    compiler may not be aware of it and might try to reuse
//    the same register for another argument which has constraints
//    that allow it ("r" for example).

inline uint64_t __sync_val_compare_and_swap(volatile uint64_t* ptr,
                                            uint64_t old_value,
                                            uint64_t new_value)
{
    uint64_t prev;
    __asm__ __volatile__("push %%ebx\n\t"
                         "movl (%3), %%ebx\n\t"    // Move 64-bit new_value into
                         "movl 4(%3), %%ecx\n\t"   // ecx:ebx
                         "lock; cmpxchg8b (%1)\n\t"// If edx:eax (old_value) same
                         "pop %%ebx\n\t"
                         : "=A" (prev)             // as contents of ptr:
                         : "D" (ptr),              //   ecx:ebx => ptr
                           "0" (old_value),        // else:
                           "S" (&new_value)        //   old *ptr => edx:eax
                         : "memory", "%ecx");
    return prev;
}
#endif  // Compiler < gcc-4.1
#endif

inline uint32_t
compare_and_swap_32_nobarrier(volatile uint32_t* ptr, uint32_t old_value, uint32_t new_value)
{
#ifdef _MSC_VER
    return InterlockedCompareExchange(ptr, old_value, new_value);
#else
    uint32_t prev;
    __asm__ __volatile__("lock; cmpxchgl %1,%2"
                         : "=a" (prev)
                         : "q" (new_value), "m" (*ptr), "0" (old_value)
                         : "memory");
    return prev;
#endif
}

inline uint32_t
compare_and_swap_32_acquire(volatile uint32_t* ptr, uint32_t old_value, uint32_t new_value)
{
#ifdef _MSC_VER
    return InterlockedCompareExchangeAcquire(ptr, old_value, new_value);
#else
    uint32_t x = compare_and_swap_32_nobarrier(ptr, old_value, new_value);

    if (LevelDB_AtomicOps_Internalx86CPUFeatures.has_amd_lock_mb_bug)
    {
        __asm__ __volatile__("lfence" : : : "memory");
    }

    return x;
#endif
}

inline uint32_t
compare_and_swap_32_release(volatile uint32_t* ptr, uint32_t old_value, uint32_t new_value)
{
    return compare_and_swap_32_nobarrier(ptr, old_value, new_value);
}

inline uint64_t
compare_and_swap_64_nobarrier(volatile uint64_t* ptr, uint64_t old_value, uint64_t new_value)
{
#ifdef _MSC_VER
    return InterlockedCompareExchange64((volatile __int64*)ptr, old_value, new_value);
#elif defined(__x86_64__)
    uint64_t prev;
    __asm__ __volatile__("lock; cmpxchgq %1,%2"
                         : "=a" (prev)
                         : "q" (new_value), "m" (*ptr), "0" (old_value)
                         : "memory");
    return prev;
#else
    return __sync_val_compare_and_swap(ptr, old_val, new_val);
#endif
}

inline uint64_t
compare_and_swap_64_acquire(volatile uint64_t* ptr, uint64_t old_value, uint64_t new_value)
{
#ifdef _MSC_VER
    return InterlockedCompareExchangeAcquire64((volatile __int64*)ptr, old_value, new_value);
#else
    uint64_t x = compare_and_swap_64_nobarrier(ptr, old_value, new_value);

    if (LevelDB_AtomicOps_Internalx86CPUFeatures.has_amd_lock_mb_bug)
    {
        __asm__ __volatile__("lfence" : : : "memory");
    }

    return x;
#endif
}

inline uint64_t
compare_and_swap_64_release(volatile uint64_t* ptr, uint64_t old_value, uint64_t new_value)
{
#ifdef _MSC_VER
    return InterlockedCompareExchangeRelease64((volatile __int64*)ptr, old_value, new_value);
#else
    return compare_and_swap_64_nobarrier(ptr, old_value, new_value);
#endif
}

template <typename P>
inline P*
compare_and_swap_ptr_nobarrier(P* volatile* ptr, P* old_value, P* new_value)
{
#ifdef _MSC_VER
    return (P*)InterlockedCompareExchangePointer((PVOID *)ptr, (PVOID)old_value, (PVOID)new_value);
#elif defined(__x86_64__)
    P* prev;
    __asm__ __volatile__("lock; cmpxchgq %1,%2"
                         : "=a" (prev)
                         : "q" (new_value), "m" (*ptr), "0" (old_value)
                         : "memory");
    return prev;
#else
    uint32_t prev;
    __asm__ __volatile__("lock; cmpxchgl %1,%2"
                         : "=a" (prev)
                         : "q" (new_value), "m" (*ptr), "0" (old_value)
                         : "memory");
    return prev;
#endif
}

template <typename P>
inline P*
compare_and_swap_ptr_acquire(P* volatile* ptr, P* old_value, P* new_value)
{
#ifdef _MSC_VER
    return (P*)InterlockedCompareExchangePointerAcquire((PVOID *)ptr, (PVOID)old_value, (PVOID)new_value);
#else
    P* x = compare_and_swap_ptr_nobarrier(ptr, old_value, new_value);

    if (LevelDB_AtomicOps_Internalx86CPUFeatures.has_amd_lock_mb_bug)
    {
        __asm__ __volatile__("lfence" : : : "memory");
    }

    return x;
#endif
}

template <typename P>
inline P*
compare_and_swap_ptr_release(P* volatile* ptr, P* old_value, P* new_value)
{
#ifdef _MSC_VER
    return (P*)InterlockedCompareExchangePointerRelease((PVOID *)ptr, (PVOID)old_value, (PVOID)new_value);
#else
    return compare_and_swap_ptr_nobarrier(ptr, old_value, new_value);
#endif
}

template <typename P>
inline P*
compare_and_swap_ptr_fullbarrier(P* volatile* ptr, P* old_value, P* new_value)
{
    P* x = compare_and_swap_ptr_nobarrier(ptr, old_value, new_value);

    if (LevelDB_AtomicOps_Internalx86CPUFeatures.has_amd_lock_mb_bug)
    {
        __asm__ __volatile__("lfence" : : : "memory");
    }

    return x;
}

//////////////////////////////// Atomic Exchange ///////////////////////////////

inline uint32_t
exchange_32_nobarrier(volatile uint32_t* ptr, uint32_t new_value)
{
#ifdef _MSC_VER
    InterlockedExchange(ptr, new_value);
#else
    __asm__ __volatile__("xchgl %1,%0"  // The lock prefix is implicit for xchg.
                         : "=r" (new_value)
                         : "m" (*ptr), "0" (new_value)
                         : "memory");
#endif
    return new_value;  // Now it's the previous value.
}

inline uint64_t
exchange_64_nobarrier(volatile uint64_t* ptr, uint64_t new_value)
{
#ifdef _MSC_VER
    InterlockedExchange64((volatile __int64*)ptr, new_value);
#elif defined(__x86_64__)
    __asm__ __volatile__("xchgq %1,%0"  // The lock prefix is implicit for xchg.
                         : "=r" (new_value)
                         : "m" (*ptr), "0" (new_value)
                         : "memory");
    return new_value;  // Now it's the previous value.
#else
    uint64_t old_val;

    do
    {
        old_val = load_64_nobarrier(ptr);
    } while (__sync_val_compare_and_swap(ptr, old_val, new_val) != old_val);

    return old_val;
#endif
}

template <typename P>
inline P*
exchange_ptr_nobarrier(P* volatile* ptr, P* new_value)
{
#ifdef _MSC_VER
    InterlockedExchangePointer(ptr, new_value);
#elif defined(__x86_64__)
    __asm__ __volatile__("xchgq %1,%0"  // The lock prefix is implicit for xchg.
                         : "=r" (new_value)
                         : "m" (*ptr), "0" (new_value)
                         : "memory");
#else
    __asm__ __volatile__("xchgl %1,%0"  // The lock prefix is implicit for xchg.
                         : "=r" (new_value)
                         : "m" (*ptr), "0" (new_value)
                         : "memory");
#endif
    return new_value;  // Now it's the previous value.
}

/////////////////////////////// Atomic Increment ///////////////////////////////

inline uint32_t
increment_32_nobarrier(volatile uint32_t* ptr, uint32_t increment)
{
#ifdef _MSC_VER
    return InterlockedExchangeAdd((LONG *)ptr, increment);
#else
    uint32_t temp = increment;
    __asm__ __volatile__("lock; xaddl %0,%1"
                         : "+r" (temp), "+m" (*ptr)
                         : : "memory");
    // temp now holds the old value of *ptr
    return temp + increment;
#endif
}

inline uint32_t
increment_32_fullbarrier(volatile uint32_t* ptr, uint32_t increment)
{
#ifdef _MSC_VER
    return InterlockedExchangeAdd((LONG *)ptr, increment);
#else
    uint32_t temp = increment;
    __asm__ __volatile__("lock; xaddl %0,%1"
                         : "+r" (temp), "+m" (*ptr)
                         : : "memory");

    // temp now holds the old value of *ptr
    if (LevelDB_AtomicOps_Internalx86CPUFeatures.has_amd_lock_mb_bug)
    {
        __asm__ __volatile__("lfence" : : : "memory");
    }

    return temp + increment;
#endif
}

inline uint64_t
increment_64_nobarrier(volatile uint64_t* ptr, uint64_t increment)
{
#ifdef _MSC_VER
    return InterlockedExchangeAdd64((LONG64 *) ptr, increment);
#elif defined(__x86_64__)
    uint64_t temp = increment;
    __asm__ __volatile__("lock; xaddq %0,%1"
                         : "+r" (temp), "+m" (*ptr)
                         : : "memory");
    // temp now contains the previous value of *ptr
    return temp + increment;
#else
    uint64_t old_val;
    uint64_t new_val;

    do
    {
        old_val = load_64_nobarrier(ptr);
        new_val = old_val + increment;
    } while (__sync_val_compare_and_swap(ptr, old_val, new_val) != old_val);

    return old_val + increment;
#endif
}

inline uint64_t
increment_64_fullbarrier(volatile uint64_t* ptr, uint64_t increment)
{
#ifdef _MSC_VER
    return InterlockedExchangeAdd64((LONG64 *)ptr, increment);
#else
    uint64_t new_val = increment_64_nobarrier(ptr, increment);

    // temp now contains the previous value of *ptr
    if (LevelDB_AtomicOps_Internalx86CPUFeatures.has_amd_lock_mb_bug)
    {
        __asm__ __volatile__("lfence" : : : "memory");
    }

    return new_val;
#endif
}

/////////////////////////////////// Atomic Or //////////////////////////////////

inline void
or_32_nobarrier(volatile uint32_t* ptr, uint32_t orwith)
{
#ifdef _MSC_VER
    _InterlockedOr((LONG *)ptr, (LONG)orwith);
#else
    __asm__ __volatile__("lock; orl %0,%1"
                         : "+r" (orwith), "+m" (*ptr)
                         : : "memory");
#endif
}

inline void
or_64_nobarrier(volatile uint64_t* ptr, uint64_t orwith)
{
#ifdef _MSC_VER
    InterlockedOr64((__int64*)ptr, orwith);
#else
    __asm__ __volatile__("lock; orq %0,%1"
                         : "+r" (orwith), "+m" (*ptr)
                         : : "memory");
#endif
}

////////////////////////////////// Atomic And //////////////////////////////////

inline void
and_32_nobarrier(volatile uint32_t* ptr, uint32_t andwith)
{
#ifdef _MSC_VER
    _InterlockedAnd((LONG *)ptr, (LONG)andwith);
#else
    __asm__ __volatile__("lock; andl %0,%1"
                         : "+r" (andwith), "+m" (*ptr)
                         : : "memory");
#endif
}

inline void
and_64_nobarrier(volatile uint64_t* ptr, uint64_t andwith)
{
#ifdef _MSC_VER
    InterlockedAnd64((volatile __int64*)ptr, andwith);
#else
    __asm__ __volatile__("lock; andq %0,%1"
                         : "+r" (andwith), "+m" (*ptr)
                         : : "memory");
#endif
}

} // namespace atomic
} // namespace leveldb

#undef ATOMICOPS_COMPILER_BARRIER

#endif // leveldb_atomic_h_
