# RPC benchmarks

For dynamic tagged frames, concrete and erased typed calls, and TCP send costs,
see the [send-ring benchmarks](#send-ring-benchmarks). For the public APIs, see
[Typed RPC](../../docs/typed_rpc.md).
Root tests include the supported module; the standalone build below remains
available for focused validation. The historical results under `results/` retain
the original prototype paths, hashes and validation limitations.

The November 14 sandbox study used Portal `19aa5491039f47db54a3b56d2ba26ee33a5a460f`
as its base and did not have a Flux Foundry checkout. Its codec and allocation
measurements are archived separately. [November 14 report](results/2026-11-14-sandbox/report.md)

## Build from a complete checkout

```sh
cmake -S tests/typed_rpc -B build-typed -DCMAKE_BUILD_TYPE=Release \
  -DPORTAL_TYPED_RPC_BUILD_BENCHMARKS=ON
cmake --build build-typed --parallel 2
ctest --test-dir build-typed --output-on-failure
python3 benchmarks/typed_rpc/run.py \
  --binary build-typed/typed_probe \
  --allocations build-typed/typed_allocations \
  --out /tmp/portal-typed-results --repetitions 7
```

Windows multi-configuration builds use `--config Release` and binaries in the
`Release` subdirectory. MSVC/Windows has not been tested in this experiment.
The allocation-counter target needs exceptions; it is separate from the timing
binary. The source also supplies a benchmark-free strict/no-exception test gate.
No compiler, test process or second benchmark is intentionally run concurrently
with a timing phase. The runner pins child processes to an available CPU on
platforms supporting `sched_setaffinity`; pinning does not isolate a shared host.

## Explicit isolated legacy control used in the recorded sandbox

The complete repositories could not be downloaded. The recorded build enabled:

```sh
-DPORTAL_TYPED_RPC_ISOLATED_LEGACY=ON
```

That option invokes the already-existing `benchmarks/reuse_pass/extract_wire.py`
on the blob-verified current `rpc.h`. It preserves the actual codec/router and
workspace bodies, replacing unrelated backend template declarations with the
minimal trait definitions needed for the supported **legacy Binder ABI**. The
existing `rpc_reuse.h` is used directly. There is no FF stub and no invented
replacement RPC implementation. The typed headers and actual Portal
async headers compile normally, without codec extraction.

Consequently the benchmark measures codec/router + identical legacy binder body,
not generated `.def` dispatch/overload/converter performance. A full checkout
build defaults to the real original backend headers, but still intentionally
uses the same legacy Binder ABI for this comparison. The isolated recorded
measurements do not establish full repository integration.

## Lanes and their limits

| Lane | Purpose |
|---|---|
| `dynamic_fresh` | Current owning RPC request/vector/reply allocation path |
| `dynamic_reuse` | Current accepted argument-vector + byte-buffer reuse path; primary baseline |
| `legacy_static` | Same original wire bytes, known-signature server decode without dynamic argument values |
| `typed_reuse` | New checked static codecs + general typed router + explicit byte cache |
| `typed_fresh` | Same new path with zero cached capacity |
| `hand_reference` | Per-method specialized same-new-format checked reference, no general routing table |

`legacy_static` is monomorphic, not a second complete generic router. It also
uses the new checked scalar reader/writer inside the server. Thus it is evidence
about avoiding general representation/dispatch work, not a clean isolation of
one single instruction or the value container alone. `hand_reference` shares new
low-level codecs/envelope checks; it is **not an independent handwritten codec**,
a universal lower bound, or proof of minimal possible latency.

All lanes call the same noinline native arithmetic/owning echo function and
verify the result on every iteration. A noinline transport wrapper prevents the
entire loopback byte pipeline from simply collapsing into the native operation.
The lane selection occurs outside the measured loop. These choices do not
eliminate every compiler/code-layout interaction. Successful-call semantics are
matched; wire formats, error reporting and overload contracts differ, so the
new path is not advertised as a transparent old-protocol replacement.

## Measurement and retention

Each independent process warms up 4,096 calls. Seven rounds rotate/shuffle lanes
and payload cases with seed 20261114. Scalar uses two **i64**, not narrowed i32.
Measured counts: scalar 2,000,000; 16-byte 500,000; 1-KiB 200,000; 64-KiB 5,000.
Strings contain owning binary data; every echo is compared against the input.
Payload copies are not removed by changing results into borrowed views.

All cache-enabled lanes explicitly use **1 MiB per byte buffer**. Defaults remain
64 KiB; a 64-KiB payload plus headers exceeds the default. Allocation counts are
collected with a separately compiled interposition binary after warmup; timing
has no global-new interposition. Counts cover C++ allocation requests, not libc
internal allocations, system calls or RSS. Ordinary scalar reuse is already
zero-allocation, an important control.

`run.py` preserves every sample, command, stdout/stderr, checksum, environment
and binary hash. Summary rows are medians of per-process average nanoseconds,
not per-request percentiles or confidence intervals. No outliers are discarded.
The November 14 results cover in-process codec/router round trips. The send-ring
study below also measures TCP between hosts.

## Retained negative results and factor experiment

The first candidate used string resize+copy and cleared cached byte vectors.
Its 64-KiB reuse result regressed. `experiments/*.patch` transform the selected
headers **back to** each factor control; they are experiments, not additional
recommended patches. Apply them only in disposable checkout copies.

All four binaries were compiled before measurement, with GCC14.2/C++14/O3,
`-DNDEBUG -pthread`, and the same `probe.cpp` and legacy extraction. Five serial
rotating rounds then compared each factor's `dynamic_reuse` and `typed_reuse`
at 64 KiB / 10,000 operations. Raw commands, hashes and stdout are retained under
`results/2026-11-14-sandbox/factors/`. The final `selected/` acceptance study was
rebuilt/repeated after source selection; it does not reuse factor medians.

The independent allocator-environment diagnostic can be reproduced with:

```sh
python3 benchmarks/typed_rpc/allocator_control.py \
  --binary build-typed/typed_probe --out /tmp/typed-allocator-control
```

It compares default child environments and `MALLOC_TRIM_THRESHOLD_=-1` at 64 KiB,
with five rotated rounds. This is a diagnosis, not a deployment recommendation.
The library and normal runner do not alter allocator tunables.

## Send-ring benchmarks

The dynamic benchmark encodes tagged request values. The typed benchmark uses a
fixed method schema and times both a concrete invocation and the same invocation
wrapped in `portal::invocation<R>`. TCP runs measure a full byte stream with a
receiver acknowledgement at EOF. Each comparison uses the same wire bytes within
its pair.

### Run the in-process comparisons

From the repository root, build the typed probe using the standalone CMake
configuration in [Build from a complete checkout](#build-from-a-complete-checkout),
then run:

```sh
./build-typed/typed_ring_probe > typed-ring.csv

cmake -S . -B build-ring -DCMAKE_BUILD_TYPE=Release \
  -DPORTAL_BUILD_TESTS=OFF -DPORTAL_BUILD_BENCHMARKS=ON
cmake --build build-ring --target rpc_ring_benchmark --parallel 2
./build-ring/rpc_ring_benchmark --check
./build-ring/rpc_ring_benchmark staged 65536
./build-ring/rpc_ring_benchmark direct 65536
```

`typed_ring_probe` alternates staged and direct calls, warms up both modes, and
prints seven samples for each payload and invocation type. A single thread creates
the payload, starts the call, checks every ring byte and delivers an inline
acknowledgement. The dynamic benchmark runs a producer and consumer on separate
threads. It accepts `scalar`, `1024` or `65536`, checks every byte and reports
nanoseconds per frame. The staged dynamic writer reuses its scratch vector.
The default ring capacity is 1 MiB with a 256 KiB high watermark.

With Flux Foundry available at configure time, `rpc_ring_benchmark ff 1024`
exercises the async space waiter. For four streams sharing an executor, run:

```sh
cmake --build build-ring --target rpc_ring_multiflow_benchmark --parallel 2
./build-ring/rpc_ring_multiflow_benchmark cv paused 16
./build-ring/rpc_ring_multiflow_benchmark ff paused 16
```

### Run the TCP comparison

On Linux or macOS, build `rpc_ring_tcp_benchmark` from the same root configuration.
Start the receiver once per sample. Substitute its reachable IPv4 address for
`RECEIVER_IP` on both hosts.

```sh
# Receiver
python3 benchmarks/typed_rpc/ring_tcp_peer.py RECEIVER_IP 19715

# Sender
cmake --build build-ring --target rpc_ring_tcp_benchmark --parallel 2
./build-ring/rpc_ring_tcp_benchmark RECEIVER_IP 19715
```

| Sender mode before the address | Encoding and send path |
| --- | --- |
| `staged-copy` | Encode into reusable scratch storage, copy into the ring, use ordinary `sendmsg`. |
| `copy` | Encode into the ring, use ordinary `sendmsg`. |
| Omitted | Encode into the ring and use the platform default. Linux selects by send size and socket capability. macOS uses ordinary `sendmsg`. |
| `zerocopy` | Require Linux socket support and request zero copy for every submission. Counts report kernel copies and `ENOBUFS` fallbacks. |

Each run sends 512 typed frames of 65,535 bytes through a 1 MiB ring. The peer
validates every byte before acknowledging. Compare `staged-copy` with `copy` to
measure the extra staging copy, and `copy` with the default to measure the Linux
send policy. On Linux, `rpc_ring_tcp_benchmark --check` also checks completion
ranges, submission ID wraparound and producer wakeup.

### Recorded results

Medians for a 64 KiB payload in the in-process runs, in microseconds per call or
frame. The dynamic producer and consumer were restricted to CPUs 2 and 3 on the
3700X. The typed probe ran on CPU 2 on that host. The Intel Mac ran macOS 14.6.1
with Apple Clang 16. Each median covers seven samples. [Typed samples](results/2026-11-18-send-ring/typed-ring.csv) · [Dynamic samples](results/2026-11-18-send-ring/dynamic-ring.csv)

| Host | Workload | Staged | Direct |
| --- | --- | ---: | ---: |
| Ryzen 7 3700X | Dynamic tagged frame | 2.41 | 1.44 |
| Ryzen 7 3700X | Typed frame, concrete call | 5.52 | 3.57 |
| Ryzen 7 3700X | Typed frame, erased call | 5.54 | 3.62 |
| Intel i5-8259U | Typed frame, concrete call | 11.98 | 8.23 |
| Intel i5-8259U | Typed frame, erased call | 12.30 | 8.33 |

At 16 bytes on the 3700X, concrete typed calls took 120 ns staged and 138 ns
direct. Erased calls took 161 ns staged and 173 ns direct. The samples also
include 4 KiB typed payloads and dynamic scalar and 1 KiB frames.

For TCP, CPU time is sender **process CPU milliseconds per 33,553,920-byte run**.
Throughput is end-to-end Mb/s. The 3700X to Cortex-A72 run used five measured
pairs. Each Wi-Fi comparison used ten. The historical Mac staged control used
the same reusable scratch and ordinary send protocol now available as
`staged-copy`. In the CSV, repeat `-1` denotes warmup, and `Mac` is the Intel
i5-8259U. [TCP samples](results/2026-11-18-send-ring/tcp.csv)

| Sender → receiver | Comparison | Sender CPU ms | Throughput Mb/s |
| --- | --- | ---: | ---: |
| 3700X → Cortex-A72, Ethernet | Ordinary → automatic Linux zero copy | 4.72 → 2.13 | 936 → 936 |
| 3700X → Intel Mac, Wi-Fi | Ordinary → automatic Linux zero copy | 5.87 → 2.78 | 395 → 361 |
| Intel Mac → Cortex-A72, Wi-Fi | Staged → direct, ordinary sends | 20.56 → 20.39 | 456 → 473 |

On Ethernet the Linux sender used about 55% less CPU at the same line rate.
On Wi-Fi the Linux sender used less CPU in all ten pairs, while throughput
varied in both directions. With the Mac sending, direct encoding used less CPU
in three of ten pairs. The in-process Mac gain did not carry through consistently
to whole-process TCP CPU time.

## Runtime compilation and registration

The optional `runtime_registration_benchmark` compares the November 16 prototype
coordinator, the shared-worker registry, the single-owner I/O registry and a
bare typed router in the same process. All invoke the same runtime-compiled handler. Each worker uses a pre-encoded
scalar request, reuses its own response buffer, and decodes and checks every
answer. The benchmark rotates the cases over seven rounds at 100,000 calls
per worker, after worker-local warmup. The four-worker column is wall time
divided by **all** completed calls, so it measures aggregate throughput.
[Build and run](../../docs/runtime_registration.md#build-and-run) ·
[Prototype control](../experiments/runtime_registration_original.h)

| Host | Readers | Bare router (ns/call) | Original prototype | Single admission counter¹ | 16 reader slots |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ryzen 7 3700X, Linux | 1 | 21.54 | 53.95 | 27.50 | 26.78 |
| Ryzen 7 3700X, Linux | 4 | 7.30 | 216.46 | 55.04 | 8.20 |
| Intel i5-8259U, macOS | 1 | 48.93 | 146.47 | 66.15 | 57.34 |
| Intel i5-8259U, macOS | 4 | 16.35 | 494.73 | 92.19 | 22.04 |

¹ The single-counter medians come from the earlier seven-round run on each host.
The other three medians come from the reader-slot run. The shared registry uses
16 64-byte-aligned admission counters and reuses one read lease between
publications. Each request updates one counter. The publisher scans all 16.

On the 3700X, compiling and loading the sample module took a median **415 ms**.
Publishing the compiled handler into an idle registry took **1.61 µs**. On the
Intel Mac, the corresponding medians were **1.43 s** and **7.02 µs**. The
single-counter idle publish medians were 1.43 µs and 5.97 µs respectively. These
five-sample figures include compiler process startup and module loading in
`compile_load`; `publish` measures pinning, registration and reopening admission.
The dispatch timings use an already published module and exclude registration.

Host binaries used C++17, `-O3 -DNDEBUG`, GCC 16.2 on Linux (affinity mask
2–5) and Apple Clang 16 on macOS 14.6.1 (system scheduler). Module compilation
used the extension's `-O2` default. Raw times are nanoseconds per operation.
[3700X current](results/2026-11-18-runtime-registration/3700x-reader-slots.csv) ·
[Intel Mac current](results/2026-11-18-runtime-registration/i5-8259u-reader-slots.csv) ·
[3700X single counter](results/2026-11-18-runtime-registration/3700x.csv) ·
[Intel Mac single counter](results/2026-11-18-runtime-registration/i5-8259u.csv)

### Single-owner copy-on-write index

Each I/O owner performs lookup against its own current index. The control thread
can build a replacement while previously resolved calls continue on workers.
These same-run medians include decode and result validation. Four owners dispatch
independently with an index installed on each before warmup. Four-worker times
are wall time per completed call across the four owners.

| Host | Workers | Bare router (ns/call) | Shared registry, 16 slots | I/O owner, COW |
| --- | ---: | ---: | ---: | ---: |
| Ryzen 7 3700X, Linux | 1 | 23.11 | 32.50 | 24.46 |
| Ryzen 7 3700X, Linux | 4 | 7.16 | 8.49 | 7.83 |
| Intel i5-8259U, macOS | 1 | 51.60 | 63.46 | 49.11 |
| Intel i5-8259U, macOS | 4 | 17.81 | 24.07 | 17.72 |

For one precompiled module added to an empty registry, median preparation took
1.76 µs on Linux and 2.83 µs on macOS. Submission to a separate FF
`simple_executor` thread through installation and acknowledgment observed by
the submitting thread took 9.13 µs and 15.01 µs respectively (five samples).
The interval includes queueing and waking the submitter. The steady-state calls
above exclude publication and cross-thread handler scheduling.
[3700X samples](results/2026-11-18-runtime-registration/3700x-io-cow.csv) ·
[Intel Mac samples](results/2026-11-18-runtime-registration/i5-8259u-io-cow.csv)

### Portal and rpclib over TCP

Both libraries run the same two handlers, integer addition and a byte-for-byte
string echo. Each process has one client, one server worker and one outstanding
synchronous call on a persistent IPv4 localhost connection. Portal uses
`typed_rpc::call`, its default workspace, an I/O-owned COW registry and a
blocking socket transport supplied by the benchmark. rpclib uses
`client.call(...).as<T>()`, `server.bind` and `async_run(1)`, including its
client I/O thread. Both use the system TCP defaults. These stacks differ in
framing, serialization, memory reuse and scheduling. This comparison measures
their combined cost.

The tables give the median of seven per-round p50s. Each round warms up each
workload for 64 calls. The two implementations alternate which runs first,
using separate processes. Every timed call includes request encoding, response
decoding and validation. Echo checks the full contents, including embedded zero
bytes. The timed TCP samples per round are 4,000 for addition, 2,000 each for
16 B and 1 KiB, and 400 for 64 KiB.

| TCP round trip | 3700X Portal (µs) | 3700X rpclib (µs) | Intel Mac Portal (µs) | Intel Mac rpclib (µs) |
| --- | ---: | ---: | ---: | ---: |
| int32 add | 13.73 | 25.53 | 21.36 | 95.16 |
| 16 B echo | 14.13 | 26.33 | 21.06 | 95.51 |
| 1 KiB echo | 14.26 | 25.70 | 21.32 | 97.57 |
| 64 KiB echo | 73.75 | 83.48 | 62.23 | 164.35 |

Small-call TCP p50 favored Portal in every paired round on both machines. For
64 KiB on Linux, Portal led in five of seven pairs. For scalar TCP p99, the
median across rounds was 27.35 vs 67.58 µs on Linux and 53.56 vs 128.24 µs on
the Mac. The CSVs also contain per-round means and p99s.

| In-process byte round trip | 3700X Portal (ns) | 3700X rpclib (ns) | Intel Mac Portal (ns) | Intel Mac rpclib (ns) |
| --- | ---: | ---: | ---: | ---: |
| int32 add | 100 | 460 | 129 | 1,093 |
| 16 B echo | 180 | 550 | 166 | 1,180 |
| 1 KiB echo | 230 | 630 | 350 | 1,600 |
| 64 KiB echo | 6,710 | 10,850 | 13,744 | 24,390 |

The in-process workload takes ten times as many samples. Portal encodes a
fresh request and dispatches its bytes locally. The rpclib control packs the
MessagePack-RPC request into a reusable buffer, unpacks it, invokes the upstream
dispatcher, serializes its response and unpacks the result. Neither local path
involves sockets or futures. Each call includes two steady-clock reads in the
timing harness; clock overhead matters most for the smallest local numbers.

Both hosts used an unmodified rpclib v2.3.0 static Release build and a C++17
`-O3 -DNDEBUG` benchmark. Linux used a Ryzen 7 3700X, GCC 16.2.1, kernel
7.3.4-1-cachyos and process affinity 2–5. macOS 14.6.1 used an Intel i5-8259U,
Apple Clang 16 and system scheduling. These runs cover synchronous loopback
latency at concurrency one. [Pinned rpclib source](https://github.com/rpclib/rpclib/tree/a663a1598a4b419123b2e13c0ae6a39c91dcf5b8) ·
[3700X samples](results/2026-11-18-rpclib/3700x.csv) ·
[Intel Mac samples](results/2026-11-18-rpclib/i5-8259u.csv)

To reproduce, install rpclib v2.3.0 under a separate prefix, then build and
run the optional comparison target. For the Linux affinity used above, prefix
each benchmark invocation with `taskset -c 2-5`.

```sh
cmake -S . -B build-rpclib -DCMAKE_BUILD_TYPE=Release -DPORTAL_BUILD_TESTS=OFF \
  -DPORTAL_BUILD_BENCHMARKS=ON -DCMAKE_PREFIX_PATH=/path/to/rpclib-prefix
cmake --build build-rpclib --target typed_rpc_rpclib_comparison
for round in 1 2 3 4 5 6 7; do
  for path in local tcp; do
    if [ $((round % 2)) -eq 1 ]; then order='portal rpclib'; else order='rpclib portal'; fi
    for impl in $order; do
      ./build-rpclib/typed_rpc_rpclib_comparison "$impl" "$path" "$round"
    done
  done
done
```

### FF coroutine calls over TCP

The comparison target also builds the FF lanes when CMake finds
`FLUX_FOUNDRY_INCLUDE_DIR`. Each Portal request goes through
`make_direct_rpc_invocation`, a reusable FF blueprint and
`await_static_invocation`. The erased lane applies `erase_invocation` to the
same operation before awaiting it. The single client I/O owner encodes straight
into the ring and drains its spans with the platform TCP sender in copy mode.
A full ring suspends the producer on `ring_space_awaitable`. Reclaim resumes
publication. Receipt and decoding of the matching server reply completes the
RPC. The COW server executes each handler and replies in order. This benchmark
uses that ordering for correlation.

The rpclib lane submits `async_call` and retrieves every future. Both clients
send fixed batches of 1 or 32 calls over a persistent loopback connection,
validate every scalar or complete echo payload and finish the batch before
admitting the next. Batch p50 is per-request latency only at window 1.
Throughput counts validated responses per second over the timed run, including
all submission and completion work. The send ring has 128 KiB of storage and a
16 KiB high watermark to exercise suspension within a 32-call batch. A larger
individual frame can occupy an empty ring. Each workload warms up for 64 calls.
Timed call counts per round are 4,096 additions, 2,048 echoes at each of 16 B
and 1 KiB, and 512 echoes at 64 KiB. The CSV wait/resume counts exclude warmup
and cover Portal's send ring only.

Run `typed_rpc_rpclib_comparison LANE MODE ROUND WINDOW` with
`LANE=ff|ff-erased|rpclib`, `MODE=pipeline|pipeline-nodelay` and
`WINDOW=1|32`. For each of seven rounds the benchmark driver rotated lane
order, measured both windows and ran a separate process for each combination.
The default mode uses stock socket options. The diagnostic finds both
established endpoints of the private loopback connection in the benchmark
process and sets `TCP_NODELAY` on both, including for rpclib. The two hosts,
compiler settings, CPU affinity and rpclib version match the synchronous
comparison above. FF was built from revision
`030bd9e7c8e833392267832517ff352b2d49444d`.

Median of per-round batch p50s for one outstanding scalar add on default sockets:

| Host | FF typed (µs) | FF erased (µs) | rpclib async (µs) |
| --- | ---: | ---: | ---: |
| 3700X | 14.64 | 14.66 | 22.57 |
| Intel Mac | 25.75 | 26.20 | 94.90 |

At window 32 the following values are medians of seven per-round throughputs,
in thousands of validated calls per second. Both endpoints use TCP_NODELAY.

| Host | Workload | FF typed | FF erased | rpclib async |
| --- | --- | ---: | ---: | ---: |
| 3700X | Add | 154.7 | 146.2 | 90.5 |
| 3700X | 16 B echo | 148.4 | 151.2 | 89.7 |
| 3700X | 1 KiB echo | 118.2 | 122.0 | 79.6 |
| 3700X | 64 KiB echo | 30.5 | 29.5 | 14.2 |
| Intel Mac | Add | 153.4 | 140.2 | 29.3 |
| Intel Mac | 16 B echo | 158.0 | 154.9 | 29.6 |
| Intel Mac | 1 KiB echo | 141.8 | 141.1 | 28.8 |
| Intel Mac | 64 KiB echo | 27.1 | 26.9 | 9.3 |

With stock Linux socket options, a 32-add batch took about 41 ms p50 on all
three lanes (about 779 calls/s). Enabling TCP_NODELAY on both ends reduced
that batch p50 to 0.200 ms for typed FF and 0.329 ms for rpclib. The default
Intel Mac run had no comparable 41 ms plateau: typed FF reached 172.6k adds/s
and rpclib reached 30.3k. The difference between clients includes batching,
scheduling, codec and transport costs.

At window 1 the ring never suspended. At window 32 each timed FF run recorded
128 waits and 128 resumes for 2,048 one-KiB echoes, and 496 of each for 512
64-KiB echoes. All calls, including those crossing a suspension, returned their
expected values. The scalar and 16 B batches fit below the watermark.
[3700X default](results/2026-11-18-rpclib/3700x-ff-default.csv) ·
[3700X TCP_NODELAY](results/2026-11-18-rpclib/3700x-ff-nodelay.csv) ·
[Intel Mac default](results/2026-11-18-rpclib/i5-8259u-ff-default.csv) ·
[Intel Mac TCP_NODELAY](results/2026-11-18-rpclib/i5-8259u-ff-nodelay.csv)

### rpclib on the FF transport

The hybrid sends native MessagePack-RPC on the wire. Its client sizes the
request with the upstream packer, then packs it directly into the same 128 KiB
send ring as the typed client. Both use the 16 KiB watermark, FF static
invocations, ring-space awaitable, TCP sender in copy mode and response-driven
completion. The MessagePack client uses the upstream streaming unpacker and
response decoder, checks the message ID and validates every returned value.
The benchmark has one ordered server worker per connection.

| Lane | Client | Server |
| --- | --- | --- |
| `rpclib` | Upstream `async_call` and futures | Upstream `server.async_run(1)` |
| `rpclib-ff-client` | FF with rpclib MessagePack | Upstream `server.async_run(1)` |
| `rpclib-ff` | FF with rpclib MessagePack | Benchmark I/O loop with the upstream dispatcher and reply packer |
| `ff` | FF with Portal typed frames | Benchmark I/O loop with the typed COW registry |

The replacement server uses the benchmark's single synchronous I/O loop. Its
handlers run on that thread. FF schedules client calls and send-ring waits.
The MessagePack streaming reader can consume multiple messages from one socket
read. The typed reader reads the fixed header and its declared body separately.
Response packing and frame sizes also differ. Timing these lanes keeps the
wire protocol fixed across the first three while changing who runs the I/O.
The hybrid is a benchmark adapter for rpclib v2.3.0. The interoperability check
runs a stock client against the hybrid server, including pipelined 64 KiB echoes
with embedded zeroes, and an FF client against a stock server, including an
unknown method followed by a successful call.

Run `typed_rpc_rpclib_comparison --check-rpclib-ff` for interoperability.
For timing, use `typed_rpc_rpclib_comparison LANE MODE ROUND WINDOW` with the
four lanes above, `MODE=pipeline|pipeline-nodelay` and `WINDOW=1|32`. These
results use seven new rounds, rotating the four lanes' starting order each
round. CPU affinity, workloads and validation match the FF comparison above.

Window 1 with TCP_NODELAY on both sockets (µs, batch p50, seven-round medians).

| Host | Workload | Native rpclib | FF client only | FF client + our I/O server | FF typed |
| --- | --- | ---: | ---: | ---: | ---: |
| 3700X | Add | 23.10 | 15.25 | 14.61 | 14.54 |
| 3700X | 16 B echo | 21.04 | 15.96 | 14.02 | 13.83 |
| 3700X | 1 KiB echo | 23.04 | 15.91 | 14.00 | 14.13 |
| 3700X | 64 KiB echo | 64.28 | 60.50 | 80.92 | 50.58 |
| Intel Mac | Add | 94.54 | 60.68 | 28.01 | 25.40 |
| Intel Mac | 16 B echo | 94.77 | 61.67 | 29.80 | 26.43 |
| Intel Mac | 1 KiB echo | 97.24 | 65.08 | 30.87 | 27.63 |
| Intel Mac | 64 KiB echo | 163.62 | 122.52 | 94.23 | 67.79 |

Window 32 with TCP_NODELAY on both sockets (thousands of validated calls/s, seven-round medians).

| Host | Workload | Native rpclib | FF client only | FF client + our I/O server | FF typed |
| --- | --- | ---: | ---: | ---: | ---: |
| 3700X | Add | 93.79 | 132.56 | 164.63 | 158.80 |
| 3700X | 16 B echo | 87.05 | 121.75 | 162.24 | 154.39 |
| 3700X | 1 KiB echo | 80.74 | 104.03 | 132.76 | 127.94 |
| 3700X | 64 KiB echo | 14.92 | 18.55 | 17.01 | 31.48 |
| Intel Mac | Add | 29.44 | 43.21 | 199.89 | 153.29 |
| Intel Mac | 16 B echo | 29.44 | 44.36 | 209.54 | 157.11 |
| Intel Mac | 1 KiB echo | 29.03 | 41.31 | 171.26 | 142.21 |
| Intel Mac | 64 KiB echo | 9.26 | 6.84 | 19.89 | 26.48 |

The scalar latency on the 3700X falls from 23.10 to 15.25 µs with the new
client alone. The two-ended hybrid reaches 14.61 µs, close to 14.54 µs with
typed frames. At 64 KiB the same hybrid takes **80.92 µs** for a single echo,
compared with **64.28 µs** for the stock stack. On the Intel Mac, switching
only the client slows 32-call 64 KiB batches from 9.26k to 6.84k calls/s, while
switching both ends raises that result to 19.89k. The typed stack reaches
26.48k calls/s there. Small-message gains do not predict large-echo latency or
throughput; the client-only lane can be tested against an existing binary server.

With default socket settings and 32 scalar calls in flight, all four lanes on
the 3700X measured about 0.78k calls/s. On the Intel Mac, stock rpclib measured
30.51k and the two-ended hybrid 263.19k calls/s with the same window. Socket
settings matter when comparing the small-message pipeline across hosts.

Raw rounds for the [3700X with default sockets](results/2026-11-18-rpclib/3700x-rpclib-ff-default.csv),
[3700X with TCP_NODELAY](results/2026-11-18-rpclib/3700x-rpclib-ff-nodelay.csv),
[Intel Mac with default sockets](results/2026-11-18-rpclib/i5-8259u-rpclib-ff-default.csv)
and [Intel Mac with TCP_NODELAY](results/2026-11-18-rpclib/i5-8259u-rpclib-ff-nodelay.csv).

### Generated C++ imports over FF and TCP

`ff-named` calls the service's generated `client.add(a, b)` and
`client.echo(value)` through a concrete bound view. `ff-named-erased` calls
the same methods through an erased view. The controls `ff` and `ff-erased`
construct each invocation directly. All four use the same typed wire methods,
FF runner, send ring, socket loop and COW server. The client binds each view
before timing. Per-call argument capture, encoding, decoding and result checks
are included. [Service declarations](service_import.def)

Seven rounds per host used both windows, CPU affinity 2–5 on the 3700X and the
system scheduler on the Intel MacBook Pro. Both sockets used TCP_NODELAY.
Each lane ran in its own process, rotating the first lane each round. The
latency rows give the median of seven per-round batch p50s. The throughput
rows give the median number of validated calls per second in thousands.

| Host | Measurement | Direct concrete | Named concrete | Direct erased | Named erased |
| --- | --- | ---: | ---: | ---: | ---: |
| 3700X | Add, window 1 (µs) | 13.69 | 14.60 | 14.61 | 14.59 |
| 3700X | Add, window 32 (k calls/s) | 158.34 | 157.50 | 161.32 | 151.28 |
| 3700X | 64 KiB echo, window 32 (k calls/s) | 28.31 | 32.07 | 31.31 | 30.65 |
| Intel Mac | Add, window 1 (µs) | 25.25 | 26.25 | 26.31 | 26.45 |
| Intel Mac | Add, window 32 (k calls/s) | 149.57 | 149.75 | 149.70 | 147.31 |
| Intel Mac | 64 KiB echo, window 32 (k calls/s) | 26.58 | 26.00 | 26.93 | 26.23 |

At window 1, the named concrete scalar median was about 1 µs higher on each
host. At window 32 its scalar throughput was within 1% of the direct concrete
entry on both. These are end-to-end measurements. The four lanes recorded
identical backpressure counts in every round, including 496 waits and 496
resumes per 512 large echoes at window 32.

Reproduce one case with
`typed_rpc_rpclib_comparison ff-named pipeline-nodelay 1 32`. Substitute any
of the four lane names, round numbers 1–7, and windows 1 or 32.
[3700X raw rounds](results/2026-11-18-named-imports/3700x.csv) ·
[Intel Mac raw rounds](results/2026-11-18-named-imports/i5-8259u.csv)
