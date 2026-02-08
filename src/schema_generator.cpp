#include "schema_generator.hpp"
#include <limits>
#include <memory>
#include <zerialize/zerialize.hpp>
#include <zerialize/protocols/msgpack.hpp>
#include <zerialize/protocols/cbor.hpp>
#include <zerialize/protocols/flex.hpp>
#include <zerialize/protocols/zera.hpp>
#include <fstream>
#include <iostream>
#include <vector>
#include <stdexcept>
#include <cstdint>
#include <span>

namespace sidecar {

namespace {

// Infer the sidecar attribute_type string from a zerialize value.
template <typename Reader>
std::string infer_type(Reader& value, const std::string& key) {
    if (value.isBool()) {
        return "boolean";
    }
    if (value.isInt() || value.isUInt()) {
        return "integer";
    }
    if (value.isFloat()) {
        return "float";
    }
    if (value.isString()) {
        return "string";
    }
    if (value.isArray()) {
        auto sz = value.arraySize();
        if (sz > 0) {
            auto elem = value[static_cast<size_t>(0)];
            if (elem.isInt() || elem.isUInt()) {
                return "integer_list";
            }
        }
        return "string_list";
    }
    // Null or unknown — safe fallback
    std::cerr << "warning: field '" << key
              << "' is null/unknown, defaulting to string\n";
    return "string";
}

template <typename Reader>
void print_schema(Reader& reader) {
    if (!reader.isMap()) {
        throw std::runtime_error("sample file root is not a map");
    }

    std::cout << "attributes:\n";

    auto keys = reader.mapKeys();
    for (auto key_sv : keys) {
        std::string key(key_sv);
        auto value = reader[key_sv];
        std::string type = infer_type(value, key);
        std::cout << "  - name: " << key << "\n"
                  << "    type: " << type << "\n";
    }
}

} // anonymous namespace

void generate_schema(const std::string& path, binary_format format) {
    // Read entire file into memory
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file) {
        throw std::runtime_error("cannot open file: " + path);
    }

    auto size = file.tellg();
    file.seekg(0);
    std::vector<char> buf(static_cast<size_t>(size));
    file.read(buf.data(), size);

    if (!file) {
        throw std::runtime_error("failed to read file: " + path);
    }

    auto bytes = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(buf.data()), buf.size());

    switch (format) {
        case binary_format::msgpack: {
            zerialize::MsgPack::Deserializer reader(bytes);
            print_schema(reader);
            break;
        }
        case binary_format::cbor: {
            zerialize::CBOR::Deserializer reader(bytes);
            print_schema(reader);
            break;
        }
        case binary_format::flexbuffers: {
            zerialize::Flex::Deserializer reader(bytes);
            print_schema(reader);
            break;
        }
        case binary_format::zera: {
            zerialize::Zera::Deserializer reader(bytes);
            print_schema(reader);
            break;
        }
    }
}

} // namespace sidecar
