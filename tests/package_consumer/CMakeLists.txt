cmake_minimum_required(VERSION 3.16)

project(portal_package_consumer LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(PORTAL_TEST_RUNTIME_REGISTRATION "Exercise the installed optional registration component" OFF)
if(PORTAL_TEST_RUNTIME_REGISTRATION)
    find_package(portal 0.1 CONFIG REQUIRED COMPONENTS runtime_registration)
else()
    find_package(portal 0.1 CONFIG REQUIRED)
endif()

add_executable(portal_package_consumer main.cpp metadata.cpp)
target_link_libraries(portal_package_consumer PRIVATE portal::portal)
target_include_directories(portal_package_consumer PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
)
enable_testing()
add_test(NAME package_consumer COMMAND portal_package_consumer)

if(PORTAL_TEST_RUNTIME_REGISTRATION)
    add_executable(package_registration ../runtime_registration/registration_test.cpp)
    target_link_libraries(package_registration PRIVATE portal::runtime_registration)
    get_target_property(portal_includes portal::portal INTERFACE_INCLUDE_DIRECTORIES)
    list(GET portal_includes 0 portal_include)
    add_test(NAME package_registration COMMAND package_registration "${CMAKE_CXX_COMPILER}"
        "${portal_include}" "${CMAKE_CURRENT_SOURCE_DIR}/../runtime_registration/module.cpp")
    set_tests_properties(package_registration PROPERTIES TIMEOUT 90)
endif()
