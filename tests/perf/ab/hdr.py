"""Decodes the HdrHistogram that loadgen and the write probe embed in
their JSON (`histogram_b64`, values in nanoseconds), for the mean and
any percentile the JSON does not carry.

  python3 -I hdr.py <run.json>... [--op write_ack]

prints n, p50, mean and p99 of the op in microseconds. decide.py loads
this file as a module.
"""
import argparse
import base64
import json
import math
import struct
import zlib


def decode(b64):
    """Returns ([(value, count)] for non-empty buckets, median_equivalent)."""
    raw = base64.b64decode(b64)
    _, length = struct.unpack(">ii", raw[:8])
    data = zlib.decompress(raw[8:8 + length])
    (_, payload_len, _, digits, lowest, _, _) = struct.unpack(">iiiiqqd", data[:40])
    payload = data[40:40 + payload_len]
    counts = []
    i = 0
    while i < len(payload):
        shift = 0
        value = 0
        while True:
            byte = payload[i]
            i += 1
            value |= (byte & 0x7F) << shift
            shift += 7
            if not byte & 0x80 or shift >= 63:
                if shift >= 63 and byte & 0x80:
                    value |= payload[i] << 63
                    i += 1
                break
        value = (value >> 1) ^ -(value & 1)
        if value < 0:
            counts.extend([0] * (-value))
        else:
            counts.append(value)
    largest = 2 * 10 ** digits
    sub_bucket_count = 1 << math.ceil(math.log2(largest))
    unit_magnitude = int(math.floor(math.log2(lowest)))
    half_mag = int(math.log2(sub_bucket_count)) - 1
    half = sub_bucket_count // 2
    mask = (sub_bucket_count - 1) << unit_magnitude

    def value_from_index(index):
        bucket = (index >> half_mag) - 1
        sub = (index & (half - 1)) + half
        if bucket < 0:
            sub -= half
            bucket = 0
        return sub << (bucket + unit_magnitude)

    def bucket_of(value):
        ceiling = (value | mask).bit_length()
        return ceiling - unit_magnitude - (half_mag + 1)

    def median_equivalent(value):
        bucket = bucket_of(value)
        sub = value >> (bucket + unit_magnitude)
        size = 1 << (unit_magnitude + bucket + (1 if sub >= sub_bucket_count else 0))
        return value + (size >> 1)

    return [(value_from_index(k), c) for k, c in enumerate(counts) if c], median_equivalent


def op_stats(op):
    """An `operations.<op>` object: n, mean, p50 and p99 in microseconds,
    plus the decoded buckets (ns) and their median_equivalent."""
    buckets, med = decode(op["histogram_b64"])
    total = sum(c for _, c in buckets)
    if total == 0:
        raise ValueError("empty histogram")
    mean_ns = sum(med(v) * c for v, c in buckets) / total

    def pct(p):
        target = math.ceil(total * p / 100)
        seen = 0
        for v, c in buckets:
            seen += c
            if seen >= target:
                return med(v)
        return buckets[-1][0]

    p50_ns = pct(50)
    json_p50 = op["latency_us"]["p50"]
    # The JSON reports microseconds of the same histogram: a unit change
    # would show here first.
    if json_p50 > 0 and not 0.5 <= p50_ns / 1000 / json_p50 <= 2:
        raise ValueError(f"histogram p50 {p50_ns} ns does not match the JSON's {json_p50} us")
    return dict(n=total, mean=mean_ns / 1000, p50=p50_ns / 1000, p99=pct(99) / 1000,
                buckets=buckets, med=med)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("json", nargs="+")
    parser.add_argument("--op", default="write_ack")
    args = parser.parse_args()
    for path in args.json:
        with open(path) as f:
            op = json.load(f)["operations"][args.op]
        s = op_stats(op)
        print(f"{path}: n={s['n']} p50={s['p50']:.3f}us (json {op['latency_us']['p50']}) "
              f"mean={s['mean']:.2f}us p99={s['p99']:.1f}us")


if __name__ == "__main__":
    main()
