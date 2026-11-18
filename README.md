# Portal

[![CI](https://github.com/OtakuNathan/portal/actions/workflows/ci.yml/badge.svg)](https://github.com/OtakuNathan/portal/actions/workflows/ci.yml)

Portal is a C++14, header-only contract layer for crossing runtime
boundaries with ordinary C++ call syntax. It keeps callable shape, overloads,
class identity, and conversion requirements in the type system while a backend
supplies the platform-specific runtime operations.

```text
C++ callable shape x backend runtime x converter set = usable boundary call
```

The repository includes minimal CPython C API and Node-API backends, a framed
dynamic RPC backend, a typed RPC module, and a fake backend used by the core tests.

Portal was previously named Dyna Bridge. The public API now uses
`<portal/...>` headers, the `portal::` namespace and `PORTAL_*` macros.
The declaration entry point is `<portal/portal.h>`; parameter helpers live in
`<portal/argument.h>`.
CMake consumers use `find_package(portal CONFIG REQUIRED)` and link
`portal::portal`. Update consumers together; the former names are not aliases.

## Choose an export entry point

| Your callable | Registration |
| --- | --- |
| Ordinary function / function pointer | `export_name(context, module, function)` deduces the signature. |
| Lambda, including captures / function object | `export_name<Signature>(context, module, callable)` supplies the declared signature. |
| Multiple implementations for multiple declared signatures | `export_name(context, module).bind<Signature>(implementation)...commit()` |

For example, after declaring `run_arena` in the module's `.def`:

```cpp
using Signature =
    std::string(const std::string&, const std::string&);

portal::export_run_arena<Signature>(
    context, module,
    [](const std::string& csv, const std::string& config) {
        return arena::run_arena(csv, config);
    });
```

`+[]` converts a noncapturing lambda to a function pointer for signature
deduction; exporting a lambda does not require that conversion. See the
[callable export guide and complete CPython module](docs/callable_exports.md)
for captures, overload selection, converters, registration, and build commands.
The [three-library replay example](docs/replay_composition.md) combines a Python
boundary with FF computation and lease access permissions.

## Why It Exists

Portal separates three concerns that binding libraries often combine:

- The contract declares which functions, overloads, classes, constructors, and
  member functions exist.
- A backend owns runtime handles, lookup, registration, calls, and object
  lifetime policy.
- `converter<T>` specializations move value types across the boundary.

The contract is whitelist-first. An undeclared symbol cannot be discovered
through the generated API, and a type without a converter cannot enter the
value channel. Object and callback identity use dedicated typed channels rather
than pretending every runtime handle is a value conversion.

```text
Key is static. Value is dynamic. Contract lookup is compile-time.
```

This refers to C++ contract and signature selection. A backend may still resolve
an import name to a runtime handle or register an export under its generated
name. Import lookup can normally happen once when the context is created and the
handle can then be cached; callers that already supply a handle can skip it.

## Quick Start

With Python and Node.js development headers installed, build all tests and the
two runtime examples:

```sh
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DPORTAL_BUILD_EXAMPLES=ON
cmake --build build --config Release --parallel
ctest --test-dir build -C Release --output-on-failure
```

The example modules expose the same C++ `add` function and `counter` class to
Python and JavaScript. Their shared contract is
[`examples/common/export.def`](examples/common/export.def); the backend-specific
entry points are
[`examples/python/module.cpp`](examples/python/module.cpp) and
[`examples/napi/addon.cpp`](examples/napi/addon.cpp).

## Declare a Contract

Select module-local declaration tables before including the bridge:

```cpp
#define PORTAL_IMPORT_DEF "my_module/import.def"
#define PORTAL_EXPORT_DEF "my_module/export.def"
#include <portal/portal.h>
```

An export table can declare a free function and a native class:

```cpp
BEGIN_CALLABLE_GROUP(add)
    DECL_CALLABLE(int, int, unsigned)
END_CALLABLE_GROUP

BEGIN_INTERFACE(addable)
    BEGIN_MEMBER_CALLABLE_GROUP(add)
        DECL_MEMBER_FUNCTION(int, int)
    END_MEMBER_CALLABLE_GROUP
END_INTERFACE

BEGIN_CLASS(native, counter)
    IMPLEMENTS(addable)
    DECL_CONSTRUCTOR(unsigned)
END_CLASS
```

Registration uses generated, typed entry points:

```cpp
portal::export_add(ctx, module, native::add);
portal::exports::counter::register_all(ctx, module);
```

Import declarations generate normal C++ calls and receiver delegates:

```cpp
int result = portal::call_add(ctx, 12, 13u);

auto counter = portal::construct<portal::counter>(ctx, 13u);
int value = counter.add(29);
```

Interfaces are stateless capability sets shared by concrete bridge classes.
Generated classes publicly compose their declared interfaces while retaining a
single context/object or native-storage state. Export registration flattens
interface methods onto the concrete runtime class; no runtime inheritance or
interface object is created. Interface and concrete member names must be unique
within a class, and an interface may not implement another interface.

Free functions, function objects, and lambdas can fill declared export slots.
Export overloads use ordered first-viable dispatch: the core checks arity and
then probes strict `optional<T>` conversions in `.def` declaration order. The
first successful candidate wins, so declaration order is part of the public
dispatch policy:

```cpp
portal::export_calc(ctx, module)
    .bind<int(int)>(scale_by_ten)
    .bind<int(int, unsigned)>(multiply)
    .commit();
```

## X-Macro Schema

The `.def` file is not merely a workaround for missing reflection. It is the
single, replayable bridge schema. The core interprets it multiple times to
generate symbol types, names, overload type lists, class proxies, registration
code, and compile-time probes. Reflection could change how that schema is
written, but it would not remove the need for one contract to produce several
consistent projections.

Different modules select different import and export tables. The defaults are
empty, so including `portal.h` does not expose repository test declarations.

## Why C++14

The core contract deliberately targets C++14 to remain usable with older distro
and cross-compilation toolchains. Features it needs from newer standards
(`optional`, `void_t`, and conjunction-style traits) have small local polyfills
in `optional.h` and `traits.h`. This adds some implementation and diagnostic
complexity in exchange for broader compatibility; optional integrations and
benchmarks may require newer standards. Consumers remain free to build the rest
of their project with a newer standard.

## Choose an RPC style

| Wire protocol | When to use it | Public API |
| --- | --- | --- |
| Dynamic RPC | Peers exchange tagged values. Generated `.def` calls use converters and select the first viable overload in declaration order. | `<portal/backends/rpc.h>`, with reusable buffers in `<portal/backends/rpc_reuse.h>` |
| Typed RPC | Peers share a method ID, revision and value schema. The method codecs handle scalars, records and sequences directly. | `<portal/rpc/typed.h>` for synchronous calls and `<portal/rpc/typed_async.h>` for owned async calls |

Both peers must agree on the wire protocol. Both styles let the application supply
the transport. [Dynamic RPC example](docs/reference.md#rpc-backend) ·
[Typed RPC schema and synchronous example](docs/typed_rpc.md#synchronous-owning-calls)

### Concrete and type-erased async RPC

A service declared in a module's `.def` can call a typed endpoint by name.
Map each generated symbol and signature to a wire method with
`typed_rpc::direct_import_backend`, then bind the service once.
`client.add(a, b)` returns a concrete call with `bind_static_async`, or a
type-erased call with `bind_async`. Both use the same transport reservation,
wire codec and completion path. [Build a generated async client](docs/typed_rpc.md#generated-async-imports)

With typed RPC, `make_rpc_invocation<Method>` prepares an owning frame for a
byte-vector transport. `make_direct_rpc_invocation<Method>` takes ownership of
the arguments and lets a transport encode directly into reserved send space.
Both return a concrete, move-only call that you can start with
`std::move(call).start(handler)`.

When a queue needs to hold different operation types with the same result,
use `portal::invocation<R>`. For example, this function accepts a shared
transport implementing `async_round_trip_into(request, callback)`:

```cpp
#include <portal/rpc/typed_async.h>

using Add = portal::typed_rpc::method<1, 1, portal::wire::i32,
                                      portal::wire::i32, portal::wire::i32>;

template<class Transport>
portal::invocation<std::int32_t> queued_add(std::shared_ptr<Transport> transport) {
    auto call = portal::typed_rpc::make_direct_rpc_invocation<Add>(
        std::move(transport), {}, std::int32_t{20}, std::int32_t{22});
    return portal::erase_invocation(std::move(call));
}
```

Erasure hides the concrete operation type from the queue. The receiver still
sees the same typed frame and the caller still gets an `async_result<std::int32_t>`.
[Async ownership and Flux Foundry integration](docs/typed_rpc.md#async-ownership-and-the-ff-seam)

For IPv4 TCP on Linux and macOS, `<portal/rpc/tcp_sender.h>` provides one
scatter/gather send API. Linux can use `MSG_ZEROCOPY` for large submissions and
releases ring space on kernel completion. On macOS, a successful `sendmsg`
releases the accepted prefix immediately. [TCP sender and buffer lifetime](docs/typed_rpc.md#tcp-send-completion-on-linux-and-macos)

### Runtime registration

On Linux and macOS, a host can use the optional C++17
`portal::runtime_registration` component to compile a local C++ module and publish
its typed RPC handlers through Flux Foundry. With a single I/O owner,
`io_registration_registry` prepares a copy of the routing index on the control
thread and installs it on the I/O loop. An already resolved call keeps its handler
pointer across the swap. The shared-worker registry also supports concurrent
lookup on arbitrary threads. Source builds enable this component automatically
when Flux Foundry, lease and Threads are available. [Module contract, CMake setup and example](docs/runtime_registration.md)

### RPC benchmarks

For 64 KiB typed payloads on a Ryzen 7 3700X, direct encoding reduced median
in-process time from 5.52 to 3.57 µs per concrete call and from 5.54 to 3.62 µs
per erased call. At 16 bytes both direct paths took longer on that machine.
The benchmark guide includes the dynamic RPC comparison, Intel MacBook Pro
results, and separate two-host TCP measurements. [Commands and raw samples](benchmarks/typed_rpc/README.md#send-ring-benchmarks)

In a separate synchronous localhost TCP comparison on the 3700X, typed Portal
and rpclib v2.3.0 measured 13.73 and 25.53 µs p50 for integer addition, and
73.75 and 83.48 µs for a 64 KiB echo. [Linux and macOS results and benchmark setup](benchmarks/typed_rpc/README.md#portal-and-rpclib-over-tcp)

The TCP comparison also covers FF typed and erased coroutine calls, ring backpressure,
and rpclib async calls at windows of 1 and 32. [Coroutine benchmark](benchmarks/typed_rpc/README.md#ff-coroutine-calls-over-tcp) The benchmark also pairs an
FF MessagePack-RPC client with an unmodified rpclib server, and measures the
same protocol with the benchmark I/O loop running the rpclib dispatcher. [rpclib on FF](benchmarks/typed_rpc/README.md#rpclib-on-the-ff-transport)

With 32 scalar calls in flight on the 3700X, the generated concrete C++ client
completed 157.50k calls/s and the direct invocation completed 158.34k. On the
Intel Mac the corresponding rates were 149.75k and 149.57k. The benchmark also
covers erased views and the single-request latency cost. [Generated imports over FF and TCP](benchmarks/typed_rpc/README.md#generated-c-imports-over-ff-and-tcp)

## Use With CMake

As a subdirectory:

```cmake
add_subdirectory(external/portal)
target_link_libraries(my_addon PRIVATE portal::portal)
target_include_directories(my_addon PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
```

The final include directory makes module-local `.def` paths such as
`"my_module/export.def"` visible during schema expansion.

Tests, examples, and benchmarks default to off when Portal is a
subproject. For an installed package:

```sh
cmake -S . -B build -DPORTAL_BUILD_TESTS=OFF
cmake --install build --prefix /path/to/prefix
```

```cmake
find_package(portal CONFIG REQUIRED)
target_link_libraries(my_addon PRIVATE portal::portal)
```

## Scope and Status

| Area | Included status |
| --- | --- |
| Core | C++14 header-only import/export contracts, overloads, classes, callbacks, and typed object channels |
| Python | Minimal CPython C API backend with real runtime tests |
| JavaScript | Minimal Node-API backend with real Node.js runtime tests |
| Dynamic RPC | Transport-agnostic tagged frames with existing backend/converter and overload contracts |
| Typed RPC | Public C++14 codecs, protocol-v1 typed frames, owning dispatch/results, storage reuse and async adapter |
| Scheduling | Optional Python interpreter adapter; libuv loop dispatch uses Flux Foundry's executor directly |
| Value types | Built-in Python/Node converters for `int`, `unsigned`, and UTF-8 text through `std::string`; embedded NUL is preserved, while arbitrary binary data needs its own converter |
| Platforms | CI builds Debug and Release on Linux, macOS, and Windows |

This is a bridge core, not a drop-in replacement for the complete policies and
type catalogs of pybind11, nanobind, or node-addon-api. Thread attachment, the
Python GIL, Node handle scopes, scheduler choice, and platform object ownership
are not portal core policy. Backends expose the required platform primitives;
Flux Foundry runners and runtime-specific executors can compose thread affinity,
asynchronous continuation, and cancellation without reimplementing orchestration
inside every backend. Contract changes require recompilation; there is
intentionally no runtime C++ symbol registry. Runtime-language symbol resolution
still belongs to the backend, and export overloads use ordered first-viable
selection rather than C++-style conversion ranking. Descriptor returns are not
currently part of the common protocol, and the RPC backend is a focused demo,
not a production transport stack.

Published timings are microbenchmarks, not whole-application forecasts. See
the [reference and benchmark notes](docs/reference.md#benchmarks) for scenarios,
environments, and comparison caveats.
The [November boundary-cost report](benchmarks/optimization_pass/results/2026-11-13-vm/report.md)
includes the selected core/RPC changes, lifetime fixes, rejected experiments,
and complete paired measurement data.

## Documentation

- [Callable export selection and minimal module template](docs/callable_exports.md)
- [Replay composition: Portal, FF, and lease](docs/replay_composition.md)
- [Architecture and extension boundaries](docs/architecture.md)
- [Full API reference and backend walkthrough](docs/reference.md)
- [Typed RPC API, wire v1 and compatibility rules](docs/typed_rpc.md)
- [Python example](examples/python/module.cpp)
- [Node-API example](examples/napi/addon.cpp)

## License

Apache License 2.0. See [LICENSE](LICENSE).

Core/RPC no-exception support and strict/relaxed converter checks are documented in [docs/noexceptions.md](docs/noexceptions.md).

RPC-only export conversion and custom backend migration: [RPC probes](docs/rpc-probes.md).
