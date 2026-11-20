#ifndef PORTAL_FF_RUNTIME_MODULE_H
#define PORTAL_FF_RUNTIME_MODULE_H

#include <portal/rpc/typed.h>
#include <cerrno>
#include <cstdlib>
#include <system_error>
#include <dlfcn.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef __APPLE__
#include <crt_externs.h>
#endif

namespace portal {
namespace ff {
struct module_source {
    std::string file;
    std::string compiler = "c++";
    std::vector<std::string> compiler_flags;
    std::string temporary_directory = "/tmp";
};

class runtime_module {
    using entry_type = wire::errc (*)(typed_rpc::router&, void*) noexcept;
    void* handle_;
    entry_type entry_;
    runtime_module(void* handle, entry_type entry) noexcept : handle_(handle), entry_(entry) {}

public:
    runtime_module(const runtime_module&) = delete;
    ~runtime_module() {
        ::dlclose(handle_);
    }
    wire::errc register_into(typed_rpc::router& router, void* context) const noexcept {
        return entry_(router, context);
    }
    static std::shared_ptr<runtime_module> compile(const module_source& source) {
        // All paths and compiler options come from the local application.
        std::string dir = source.temporary_directory + "/portal-module-XXXXXX";
        if (!::mkdtemp(dir.data()))
            throw std::system_error(errno, std::generic_category(), "mkdtemp");
        struct build_files {
            const std::string& directory;
            std::string library;
            ~build_files() {
                if (!library.empty())
                    ::unlink(library.c_str());
                ::rmdir(directory.c_str());
            }
        } files{dir, {}};
        files.library = dir + "/module.so";

        std::vector<std::string> args{source.compiler, "-std=c++17", "-O2", "-fPIC",
#ifdef __APPLE__
                                      "-dynamiclib"
#else
                                      "-shared"
#endif
        };
#ifdef NDEBUG
        args.emplace_back("-DNDEBUG");
#endif
        args.insert(args.end(), source.compiler_flags.begin(), source.compiler_flags.end());
        args.insert(args.end(), {source.file, "-o", files.library});
        std::vector<char*> argv;
        for (auto& arg : args)
            argv.push_back(arg.data());
        argv.push_back(nullptr);
#ifdef __APPLE__
        auto environment = *_NSGetEnviron();
#else
        auto environment = ::environ;
#endif
        pid_t pid;
        int rc = ::posix_spawnp(&pid, argv.front(), nullptr, nullptr, argv.data(), environment);
        if (rc)
            throw std::system_error(rc, std::generic_category(), "launch runtime compiler");
        int status;
        while (::waitpid(pid, &status, 0) < 0)
            if (errno != EINTR)
                throw std::system_error(errno, std::generic_category(), "waitpid");
        if (!WIFEXITED(status) || WEXITSTATUS(status))
            throw std::runtime_error("runtime compiler failed for " + source.file);

        auto close_handle = [](void* h) {
            ::dlclose(h);
        };
        std::unique_ptr<void, decltype(close_handle)> handle(
            ::dlopen(files.library.c_str(), RTLD_NOW | RTLD_LOCAL), close_handle);
        if (!handle)
            throw std::runtime_error(::dlerror());
        ::dlerror();
        auto entry = reinterpret_cast<entry_type>(::dlsym(handle.get(), "portal_register_rpc_v1"));
        if (const char* error = ::dlerror())
            throw std::runtime_error(error);
        if (!entry)
            throw std::runtime_error("null portal_register_rpc_v1");
        std::unique_ptr<runtime_module> module(new runtime_module(handle.get(), entry));
        handle.release();
        return std::shared_ptr<runtime_module>(std::move(module));
    }
};
} // namespace ff
} // namespace portal
#endif
