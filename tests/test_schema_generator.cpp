#include "schema_generator.hpp"
#include <gtest/gtest.h>
#include <zerialize/zerialize.hpp>
#include <zerialize/dynamic.hpp>
#include <zerialize/protocols/msgpack.hpp>
#include <zerialize/protocols/cbor.hpp>
#include <zerialize/protocols/flex.hpp>
#include <zerialize/protocols/zera.hpp>
#include <zerialize/protocols/ion.hpp>
#include <zerialize/protocols/bson.hpp>
#include <zerialize/protocols/beve.hpp>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

namespace {

// Writes a zerialize-serialized payload to a uniquely-named temp file for
// the lifetime of the object and removes it on destruction.
class temp_binary_file {
public:
    explicit temp_binary_file(const zerialize::ZBuffer& buf) : m_path(make_path()) {
        std::ofstream out(m_path, std::ios::binary);
        out.write(reinterpret_cast<const char*>(buf.data()),
                  static_cast<std::streamsize>(buf.size()));
    }
    ~temp_binary_file() { std::error_code ec; std::filesystem::remove(m_path, ec); }

    temp_binary_file(const temp_binary_file&) = delete;
    temp_binary_file& operator=(const temp_binary_file&) = delete;

    const std::string& path() const { return m_path_str; }

private:
    static std::filesystem::path make_path() {
        auto dir = std::filesystem::temp_directory_path();
        return dir / ("sidecar_test_schema_" + std::to_string(m_counter.fetch_add(1)) + ".bin");
    }

    static inline std::atomic<int> m_counter{0};
    std::filesystem::path m_path;
    std::string m_path_str = m_path.string();
};

// Redirects std::cout to an internal buffer for the object's lifetime.
class capture_stdout {
public:
    capture_stdout() : m_old(std::cout.rdbuf(m_buf.rdbuf())) {}
    ~capture_stdout() { std::cout.rdbuf(m_old); }

    capture_stdout(const capture_stdout&) = delete;
    capture_stdout& operator=(const capture_stdout&) = delete;

    std::string str() const { return m_buf.str(); }

private:
    std::ostringstream m_buf;
    std::streambuf* m_old;
};

} // namespace

TEST(schema_generator, infers_types_across_all_fields) {
    auto payload = zerialize::dyn::map({
        {"active", true},
        {"age", 42},
        {"price", 19.99},
        {"name", std::string("widget")},
        {"scores", zerialize::dyn::array({1, 2, 3})},
        {"tags", zerialize::dyn::array({std::string("a"), std::string("b")})},
    });
    temp_binary_file file(zerialize::serialize<zerialize::MsgPack>(payload));

    capture_stdout out;
    sidecar::generate_schema(file.path(), sidecar::binary_format::msgpack);
    auto text = out.str();

    EXPECT_NE(text.find("- name: active\n    type: boolean"), std::string::npos);
    EXPECT_NE(text.find("- name: age\n    type: integer"), std::string::npos);
    EXPECT_NE(text.find("- name: price\n    type: float"), std::string::npos);
    EXPECT_NE(text.find("- name: name\n    type: string"), std::string::npos);
    EXPECT_NE(text.find("- name: scores\n    type: integer_list"), std::string::npos);
    EXPECT_NE(text.find("- name: tags\n    type: string_list"), std::string::npos);
}

TEST(schema_generator, works_across_all_binary_formats) {
    auto payload = zerialize::dyn::map({{"value", 7}});

    {
        temp_binary_file file(zerialize::serialize<zerialize::MsgPack>(payload));
        capture_stdout out;
        sidecar::generate_schema(file.path(), sidecar::binary_format::msgpack);
        EXPECT_NE(out.str().find("type: integer"), std::string::npos);
    }
    {
        temp_binary_file file(zerialize::serialize<zerialize::CBOR>(payload));
        capture_stdout out;
        sidecar::generate_schema(file.path(), sidecar::binary_format::cbor);
        EXPECT_NE(out.str().find("type: integer"), std::string::npos);
    }
    {
        temp_binary_file file(zerialize::serialize<zerialize::Flex>(payload));
        capture_stdout out;
        sidecar::generate_schema(file.path(), sidecar::binary_format::flexbuffers);
        EXPECT_NE(out.str().find("type: integer"), std::string::npos);
    }
    {
        temp_binary_file file(zerialize::serialize<zerialize::Zera>(payload));
        capture_stdout out;
        sidecar::generate_schema(file.path(), sidecar::binary_format::zera);
        EXPECT_NE(out.str().find("type: integer"), std::string::npos);
    }
    {
        temp_binary_file file(zerialize::serialize<zerialize::Ion>(payload));
        capture_stdout out;
        sidecar::generate_schema(file.path(), sidecar::binary_format::ion);
        EXPECT_NE(out.str().find("type: integer"), std::string::npos);
    }
    {
        temp_binary_file file(zerialize::serialize<zerialize::Bson>(payload));
        capture_stdout out;
        sidecar::generate_schema(file.path(), sidecar::binary_format::bson);
        EXPECT_NE(out.str().find("type: integer"), std::string::npos);
    }
    {
        temp_binary_file file(zerialize::serialize<zerialize::Beve>(payload));
        capture_stdout out;
        sidecar::generate_schema(file.path(), sidecar::binary_format::beve);
        EXPECT_NE(out.str().find("type: integer"), std::string::npos);
    }
}

TEST(schema_generator, empty_array_defaults_to_string_list) {
    auto payload = zerialize::dyn::map({{"empty", zerialize::dyn::array({})}});
    temp_binary_file file(zerialize::serialize<zerialize::MsgPack>(payload));

    capture_stdout out;
    sidecar::generate_schema(file.path(), sidecar::binary_format::msgpack);
    EXPECT_NE(out.str().find("- name: empty\n    type: string_list"), std::string::npos);
}

TEST(schema_generator, null_field_defaults_to_string) {
    auto payload = zerialize::dyn::map({{"mystery", zerialize::dyn::Value(zerialize::dyn::Null{})}});
    temp_binary_file file(zerialize::serialize<zerialize::MsgPack>(payload));

    capture_stdout out;
    sidecar::generate_schema(file.path(), sidecar::binary_format::msgpack);
    EXPECT_NE(out.str().find("- name: mystery\n    type: string"), std::string::npos);
}

TEST(schema_generator, nonexistent_file_throws) {
    EXPECT_THROW(
        sidecar::generate_schema("/nonexistent/path/does-not-exist.bin", sidecar::binary_format::msgpack),
        std::runtime_error);
}

TEST(schema_generator, non_map_root_throws) {
    auto payload = zerialize::dyn::array({1, 2, 3});
    temp_binary_file file(zerialize::serialize<zerialize::MsgPack>(payload));

    EXPECT_THROW(
        sidecar::generate_schema(file.path(), sidecar::binary_format::msgpack),
        std::runtime_error);
}
