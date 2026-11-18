# Runtime registration of typed RPC handlers

A long-lived host can compile a local C++ translation unit, load it as a shared
library and add its handlers to a live typed RPC registry. Existing calls can
continue during compilation. The optional C++17 component runs on Linux and
macOS and uses Flux Foundry, lease, an installed C++ compiler and the system
dynamic loader. The core `portal::portal` target continues to require C++14.

## Build and run

From the Portal checkout, point CMake at the directories containing `flow/flow.h`
and `lease.hpp`. The source build also searches sibling checkouts. The default
(`PORTAL_BUILD_RUNTIME_REGISTRATION=AUTO`) enables the extension on Linux and
macOS when its dependencies are available. Set it to `ON` to require the
extension at configure time, as below, or `OFF` to skip it.

```sh
cmake -S . -B build-registration -DCMAKE_BUILD_TYPE=Release \
  -DPORTAL_BUILD_RUNTIME_REGISTRATION=ON -DPORTAL_BUILD_TESTS=ON \
  -DPORTAL_BUILD_BENCHMARKS=ON \
  -DFLUX_FOUNDRY_INCLUDE_DIR=/path/to/flux_foundry \
  -DPORTAL_LEASE_INCLUDE_DIR=/path/to/lease/lease
cmake --build build-registration --target runtime_registration_test runtime_registration_benchmark
ctest --test-dir build-registration -R '^runtime_registration$' --output-on-failure
./build-registration/runtime_registration_benchmark "$(command -v c++)" "$PWD" \
  "$PWD/tests/runtime_registration/module.cpp"
```

For an installed package, supply the same two dependency locations when
configuring the consumer. Link only consumers that use the extension.

```cmake
find_package(portal CONFIG REQUIRED COMPONENTS runtime_registration)
target_link_libraries(my_host PRIVATE portal::runtime_registration)
```

## Module and host

The loader looks up one C-linkage entry with the following signature:

```cpp
extern "C" portal::wire::errc portal_register_rpc_v1(
    portal::typed_rpc::router& router, void* context) noexcept;
```

The entry calls `router.define<Method>(handler)` and returns the status. The
host and module share C++ objects across this boundary and need matching
Portal headers, standard library ABI and compatible compiler flags. Clients
call the method by its agreed typed RPC schema. The example registers a
saturating integer addition at method ID 100. [Complete module](../tests/runtime_registration/module.cpp)

```cpp
#include <portal/extensions/flux_foundry/runtime_registration.h>

namespace ext = portal::ff;
using status = ext::registration_registry::update_result;
struct receiver {
    using value_type = flux_foundry::result_t<status, std::exception_ptr>;
    status* published;
    std::exception_ptr* failure;
    void emplace(value_type&& result) noexcept {
        if (result.has_value()) *published = result.value();
        else *failure = std::move(result).error();
    }
};

ext::registration_registry service;
auto flow = ext::registration_blueprint(service);

void add_local_module(const std::string& source, const std::string& include_dir) {
    status published = status::rejected;
    std::exception_ptr failure;
    auto run = flux_foundry::make_fast_runner_view(flow, receiver{&published, &failure});
    run(ext::module_source{source, "c++", {"-I" + include_dir}});
    if (failure) std::rethrow_exception(failure);
    if (published != status::ok) throw std::runtime_error("registration declined");
}
```

The runner executes both stages synchronously on its calling thread. Run it on
a control thread when compiler latency matters. Source, compiler, flags and
the writable temporary directory are host-controlled inputs. The loader invokes
the compiler with an argument vector, loads the image from a private directory
and removes the temporary library and directory after loading. Applications
can also retain `runtime_module::compile(job)` and publish the same module into
another registry using `publish(module, callback)`.

## Single I/O thread

Construct `io_registration_registry` on the thread that owns lookup. Start the
blueprint from a control thread. The first stages compile the module, invoke its
registration entry against an empty batch router and merge sorted route keys
into a fresh index. The last stage posts installation to the I/O executor.
A successful FF receiver runs on the owner after the new methods are visible.

```cpp
// On the I/O thread, with a Flux Foundry uv_executor named io.
portal::ff::io_registration_registry service;
auto flow = portal::ff::registration_blueprint(service, io);

// On the control thread, using a receiver for this output type.
using output = flux_foundry::result_t<portal::ff::io_registration_registry::status,
                                     std::exception_ptr>;
auto runner = flux_foundry::make_fast_runner_view(flow, receiver);
runner(portal::ff::module_source{source, "c++", {"-I" + include_dir}});
```

Call `service.dispatch(frame, reply)` on the owner, or call
`auto request = service.resolve(portal::wire::view(frame))` there and hand
`request` to a worker for `request.dispatch_into(reply)`. A resolved request
borrows the frame bytes and the handler. Keep the frame storage alive and
unmodified until invocation finishes. Each successful batch owns the handlers
and their module pin until registry destruction. Installation moves the new
index into place and releases the old index on the owner, including when an
earlier handler is still running. Lookup updates no shared state.

A competing preparation returns `busy` while a batch is being prepared or awaits
installation. A duplicate key, or a module returning a registration error,
rejects the whole batch with `rejected`. Exceptions travel in the FF error
channel. The executor transfers accepted tasks to the owner through
`dispatch(task&&) noexcept`. The adapter prefers `try_dispatch` when available
so bounded executors can reject a full or closed queue. Rejection and destruction
of an unexecuted task yield `not_delivered`. Keep a dispatch-only executor open
for submission according to its own shutdown contract. An install attempted on another
thread yields `wrong_thread`. Failed or discarded batches release their pins
and allow a retry. Keep the executor, blueprint and registry alive until all
queued registrations complete, and join handler calls before teardown.

Hosts with their own control queue can call `service.prepare(module, define)`,
move the returned update to the I/O thread and call `std::move(update).install()`.
The callback defines only this batch's methods. Destroying an uninstalled update
abandons it. Preparation builds the replacement index in O(existing + added)
time after the definitions are collected. The published registry retains one
index plus stable handler storage.

## Shared-worker dispatch and lifetime

- Call `service.dispatch(request, reply, limits)` on a worker. Each thread picks one
  of 16 64-byte-aligned slots on first dispatch and tracks its active calls in
  that slot. Threads sharing a slot and nested calls each count separately.
  A shared read lease covers all admitted calls until the next publication.
  A publisher closes every slot, waits for their counts to reach zero, then
  releases the lease and updates the router. New arrivals at a closed slot wait
  on a condition variable. The registry spends 1 KiB on slot storage. During
  dispatch, `state().active` samples the slots in succession.
- Publication returns `ok`, `rejected` for a registration error, `busy` for a
  competing publisher, `timeout` on drain timeout, or `reentrant` for a publish
  attempted from the same registry's synchronous dispatch/registration chain.
  A nested dispatch behind its own writer receives `handler_failed`.
  A registration callback must avoid joining work queued behind its own gate. The
  timeout covers draining readers. Compilation and the registration callback
  have their own execution time. The gate reopens on timeout and exceptions.
- Registration callbacks may append methods and must keep router references
  inside the callback. Successful appends survive a subsequent callback error.
  The registry pins the module before invoking registration and destroys all
  handlers before releasing module pins. A duplicate with no appends releases
  the attempted pin. A throwing callback conservatively retains its pin.
- Stop and join callers before destroying the registry. The owner supplies any
  external queuing limits and keeps handler contexts alive through handler
  destruction. Module handles are closed after handler destruction. The platform
  loader controls when the image is unmapped. The extension provides append-only
  registration.

The integration test covers compilation and symbol lookup failure, concurrent
dispatch/publication, partial registration, duplicates, reentrancy, timeouts
and module lifetime. [Integration test](../tests/runtime_registration/registration_test.cpp)
