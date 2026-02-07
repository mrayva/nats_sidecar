set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Linux)

# CMake 3.31+ defaults to CMAKE_C_EXTENSIONS=OFF, which passes -std=c11
# to the compiler. This breaks OpenSSL's inline assembly and POSIX types.
set(VCPKG_C_FLAGS "-std=gnu11")
set(VCPKG_CXX_FLAGS "")
