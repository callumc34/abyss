#pragma once

// Clang thread safety analysis annotations.
// No-ops on compilers that don't support them.

#ifdef __clang__
#define ABYSS_CAPABILITY(x) __attribute__((capability(x)))
#define ABYSS_SCOPED_CAPABILITY __attribute__((scoped_lockable))
#define ABYSS_GUARDED_BY(x) __attribute__((guarded_by(x)))
#define ABYSS_PT_GUARDED_BY(x) __attribute__((pt_guarded_by(x)))
#define ABYSS_REQUIRES(...) __attribute__((requires_capability(__VA_ARGS__)))
#define ABYSS_ACQUIRE(...) __attribute__((acquire_capability(__VA_ARGS__)))
#define ABYSS_RELEASE(...) __attribute__((release_capability(__VA_ARGS__)))
#define ABYSS_EXCLUDES(...) __attribute__((locks_excluded(__VA_ARGS__)))
#define ABYSS_NO_THREAD_SAFETY_ANALYSIS __attribute__((no_thread_safety_analysis))
#else
#define ABYSS_CAPABILITY(x)
#define ABYSS_SCOPED_CAPABILITY
#define ABYSS_GUARDED_BY(x)
#define ABYSS_PT_GUARDED_BY(x)
#define ABYSS_REQUIRES(...)
#define ABYSS_ACQUIRE(...)
#define ABYSS_RELEASE(...)
#define ABYSS_EXCLUDES(...)
#define ABYSS_NO_THREAD_SAFETY_ANALYSIS
#endif
