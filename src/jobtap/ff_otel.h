#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void *ff_otel_span_t;

int ff_otel_init(const char *service_name, const char *endpoint);
ff_otel_span_t ff_otel_span_start(const char *name);
void ff_otel_span_end(ff_otel_span_t span);

void ff_otel_span_set_attr_str(ff_otel_span_t span,
                               const char *key,
                               const char *value);

void ff_otel_span_set_attr_u64(ff_otel_span_t span,
                               const char *key,
                               uint64_t value);

#ifdef __cplusplus
}
#endif