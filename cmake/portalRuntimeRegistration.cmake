set(portal_runtime_registration_FOUND FALSE)
if(NOT CMAKE_SYSTEM_NAME MATCHES "^(Linux|Darwin)$")
    set(portal_NOT_FOUND_MESSAGE "Runtime registration requires Linux or macOS")
    return()
endif()
find_path(FLUX_FOUNDRY_INCLUDE_DIR flow/flow.h HINTS "${CMAKE_CURRENT_SOURCE_DIR}/../flux_foundry")
find_path(PORTAL_LEASE_INCLUDE_DIR lease.hpp HINTS "${CMAKE_CURRENT_SOURCE_DIR}/../lease/lease")
find_package(Threads QUIET)
if(NOT EXISTS "${FLUX_FOUNDRY_INCLUDE_DIR}/flow/flow.h" OR
   NOT EXISTS "${PORTAL_LEASE_INCLUDE_DIR}/lease.hpp" OR NOT Threads_FOUND)
    set(portal_NOT_FOUND_MESSAGE "Runtime registration requires Flux Foundry, lease and Threads")
    return()
endif()
if(NOT TARGET portal::runtime_registration)
    add_library(portal::runtime_registration INTERFACE IMPORTED)
    set_target_properties(portal::runtime_registration PROPERTIES
        INTERFACE_COMPILE_FEATURES cxx_std_17
        INTERFACE_INCLUDE_DIRECTORIES "${FLUX_FOUNDRY_INCLUDE_DIR};${PORTAL_LEASE_INCLUDE_DIR}"
        INTERFACE_LINK_LIBRARIES "portal::portal;Threads::Threads;${CMAKE_DL_LIBS}")
endif()
set(portal_runtime_registration_FOUND TRUE)
