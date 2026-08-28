#include "arrow_columnar_rows.hpp"
#include "event_bridge.hpp"
#include "config.hpp"
#include "subscription_manager.hpp"
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/null_sink.h>
#include <zerialize/zerialize.hpp>
#include <zerialize/protocols/msgpack.hpp>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <algorithm>
#include <optional>

// ArrowColumnarRows must satisfy zerialize's structural Reader concept for it to plug into the
// generic matching pipeline at all - checked here so a regression fails immediately with a clear
// message instead of a wall of errors deep inside match_columnar_batch/write_value.
static_assert(zerialize::Reader<sidecar::ArrowColumnarRows>,
              "ArrowColumnarRows must satisfy zerialize::Reader");

namespace {

auto make_log() {
    return std::make_shared<spdlog::logger>("test", std::make_shared<spdlog::sinks::null_sink_mt>());
}

bool contains(const std::vector<uint64_t>& ids, uint64_t id) {
    return std::find(ids.begin(), ids.end(), id) != ids.end();
}

// Serializes a schema + already-built arrays into a single-RecordBatch Arrow IPC *stream*
// (MakeStreamWriter, never NewFileWriter - no footer/magic bytes) - the exact shape pg_arrow's
// own rows_to_arrow() produces (confirmed by reading pg_arrow.cpp before writing this).
std::vector<char> build_arrow_ipc_stream(
    const std::vector<std::string>& names,
    const std::vector<std::shared_ptr<arrow::Array>>& arrays) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (std::size_t i = 0; i < names.size(); ++i) {
        fields.push_back(arrow::field(names[i], arrays[i]->type(), /*nullable=*/true));
    }
    auto schema = arrow::schema(fields);
    int64_t num_rows = arrays.empty() ? 0 : arrays[0]->length();
    auto batch = arrow::RecordBatch::Make(schema, num_rows, arrays);

    auto sink = *arrow::io::BufferOutputStream::Create();
    auto writer = *arrow::ipc::MakeStreamWriter(sink, schema);
    EXPECT_TRUE(writer->WriteRecordBatch(*batch).ok());
    EXPECT_TRUE(writer->Close().ok());
    auto buffer = *sink->Finish();
    return std::vector<char>(
        reinterpret_cast<const char*>(buffer->data()),
        reinterpret_cast<const char*>(buffer->data()) + buffer->size());
}

// A schema-only IPC stream with no RecordBatch at all - the documented zero-field-schema edge
// case (ReadNext() returns a null batch even on success; see pg_arrow.cpp's own arrow_to_jsonb()
// and arrow_columnar_rows.hpp's constructor comment for the identical check).
std::vector<char> build_arrow_ipc_stream_no_batch() {
    auto schema = arrow::schema({});
    auto sink = *arrow::io::BufferOutputStream::Create();
    auto writer = *arrow::ipc::MakeStreamWriter(sink, schema);
    EXPECT_TRUE(writer->Close().ok());
    auto buffer = *sink->Finish();
    return std::vector<char>(
        reinterpret_cast<const char*>(buffer->data()),
        reinterpret_cast<const char*>(buffer->data()) + buffer->size());
}

std::shared_ptr<arrow::Array> int64_array(const std::vector<std::optional<int64_t>>& values) {
    arrow::Int64Builder builder;
    for (auto& v : values) {
        if (v) EXPECT_TRUE(builder.Append(*v).ok());
        else EXPECT_TRUE(builder.AppendNull().ok());
    }
    std::shared_ptr<arrow::Array> arr;
    EXPECT_TRUE(builder.Finish(&arr).ok());
    return arr;
}

std::shared_ptr<arrow::Array> double_array(const std::vector<std::optional<double>>& values) {
    arrow::DoubleBuilder builder;
    for (auto& v : values) {
        if (v) EXPECT_TRUE(builder.Append(*v).ok());
        else EXPECT_TRUE(builder.AppendNull().ok());
    }
    std::shared_ptr<arrow::Array> arr;
    EXPECT_TRUE(builder.Finish(&arr).ok());
    return arr;
}

std::shared_ptr<arrow::Array> bool_array(const std::vector<std::optional<bool>>& values) {
    arrow::BooleanBuilder builder;
    for (auto& v : values) {
        if (v) EXPECT_TRUE(builder.Append(*v).ok());
        else EXPECT_TRUE(builder.AppendNull().ok());
    }
    std::shared_ptr<arrow::Array> arr;
    EXPECT_TRUE(builder.Finish(&arr).ok());
    return arr;
}

std::shared_ptr<arrow::Array> string_array(const std::vector<std::optional<std::string>>& values) {
    arrow::StringBuilder builder;
    for (auto& v : values) {
        if (v) EXPECT_TRUE(builder.Append(*v).ok());
        else EXPECT_TRUE(builder.AppendNull().ok());
    }
    std::shared_ptr<arrow::Array> arr;
    EXPECT_TRUE(builder.Finish(&arr).ok());
    return arr;
}

std::shared_ptr<arrow::Array> binary_array(const std::vector<std::optional<std::string>>& values) {
    arrow::BinaryBuilder builder;
    for (auto& v : values) {
        if (v) EXPECT_TRUE(builder.Append(*v).ok());
        else EXPECT_TRUE(builder.AppendNull().ok());
    }
    std::shared_ptr<arrow::Array> arr;
    EXPECT_TRUE(builder.Finish(&arr).ok());
    return arr;
}

std::shared_ptr<arrow::Array> decimal128_array(
    const std::vector<std::optional<std::string>>& values, int32_t precision, int32_t scale) {
    auto type = arrow::decimal128(precision, scale);
    arrow::Decimal128Builder builder(type);
    for (auto& v : values) {
        if (v) {
            auto dec = arrow::Decimal128::FromString(*v);
            EXPECT_TRUE(dec.ok());
            EXPECT_TRUE(builder.Append(*dec).ok());
        } else {
            EXPECT_TRUE(builder.AppendNull().ok());
        }
    }
    std::shared_ptr<arrow::Array> arr;
    EXPECT_TRUE(builder.Finish(&arr).ok());
    return arr;
}

std::shared_ptr<arrow::Array> date32_array(const std::vector<int32_t>& values) {
    arrow::Date32Builder builder;
    for (auto v : values) EXPECT_TRUE(builder.Append(v).ok());
    std::shared_ptr<arrow::Array> arr;
    EXPECT_TRUE(builder.Finish(&arr).ok());
    return arr;
}

std::span<const char> as_span(const std::vector<char>& v) {
    return std::span<const char>(v.data(), v.size());
}

} // namespace

// --- ArrowColumnarRows adapter, tested directly (no matching_engine involved) ---

TEST(arrow_columnar_rows, round_trip_int64_with_nulls) {
    auto stream = build_arrow_ipc_stream({"value"}, {int64_array({42, std::nullopt, -7})});
    sidecar::ArrowColumnarRows rows(as_span(stream));
    ASSERT_EQ(rows.size(), 3u);
    ASSERT_TRUE(rows.isMap());
    ASSERT_TRUE(rows.contains("value"));

    std::vector<int64_t> seen;
    std::vector<bool> nulls;
    for (auto& row : rows) {
        auto cell = row["value"];
        nulls.push_back(cell.isNull());
        if (!cell.isNull()) {
            EXPECT_TRUE(cell.isInt());
            seen.push_back(cell.asInt64());
        }
    }
    ASSERT_EQ(seen.size(), 2u);
    EXPECT_EQ(seen[0], 42);
    EXPECT_EQ(seen[1], -7);
    ASSERT_EQ(nulls.size(), 3u);
    EXPECT_FALSE(nulls[0]);
    EXPECT_TRUE(nulls[1]);
    EXPECT_FALSE(nulls[2]);
}

TEST(arrow_columnar_rows, round_trip_double) {
    auto stream = build_arrow_ipc_stream({"price"}, {double_array({3.5, 100.0})});
    sidecar::ArrowColumnarRows rows(as_span(stream));
    std::vector<double> seen;
    for (auto& row : rows) {
        auto cell = row["price"];
        EXPECT_TRUE(cell.isFloat());
        seen.push_back(cell.asDouble());
    }
    ASSERT_EQ(seen.size(), 2u);
    EXPECT_DOUBLE_EQ(seen[0], 3.5);
    EXPECT_DOUBLE_EQ(seen[1], 100.0);
}

TEST(arrow_columnar_rows, round_trip_bool) {
    auto stream = build_arrow_ipc_stream({"flag"}, {bool_array({true, false})});
    sidecar::ArrowColumnarRows rows(as_span(stream));
    std::vector<bool> seen;
    for (auto& row : rows) {
        auto cell = row["flag"];
        EXPECT_TRUE(cell.isBool());
        seen.push_back(cell.asBool());
    }
    ASSERT_EQ(seen.size(), 2u);
    EXPECT_TRUE(seen[0]);
    EXPECT_FALSE(seen[1]);
}

TEST(arrow_columnar_rows, round_trip_string) {
    auto stream = build_arrow_ipc_stream({"symbol"}, {string_array({std::string("AAPL"), std::string("GOOG")})});
    sidecar::ArrowColumnarRows rows(as_span(stream));
    std::vector<std::string> seen;
    for (auto& row : rows) {
        auto cell = row["symbol"];
        EXPECT_TRUE(cell.isString());
        seen.push_back(std::string(cell.asStringView()));
    }
    ASSERT_EQ(seen.size(), 2u);
    EXPECT_EQ(seen[0], "AAPL");
    EXPECT_EQ(seen[1], "GOOG");
}

TEST(arrow_columnar_rows, round_trip_binary_as_string) {
    std::string raw{"\x00\x01\xff", 3};
    auto stream = build_arrow_ipc_stream({"blob"}, {binary_array({raw})});
    sidecar::ArrowColumnarRows rows(as_span(stream));
    auto it = rows.begin();
    auto cell = (*it)["blob"];
    EXPECT_TRUE(cell.isString());
    EXPECT_EQ(std::string(cell.asStringView()), raw);
}

TEST(arrow_columnar_rows, round_trip_decimal128_as_string) {
    auto stream = build_arrow_ipc_stream({"amount"}, {decimal128_array({std::string("123.45")}, 10, 2)});
    sidecar::ArrowColumnarRows rows(as_span(stream));
    auto it = rows.begin();
    auto cell = (*it)["amount"];
    EXPECT_TRUE(cell.isString());
    EXPECT_EQ(std::string(cell.asStringView()), "123.45");
}

TEST(arrow_columnar_rows, empty_batch_zero_rows) {
    auto stream = build_arrow_ipc_stream({"value"}, {int64_array({})});
    sidecar::ArrowColumnarRows rows(as_span(stream));
    EXPECT_EQ(rows.size(), 0u);
    EXPECT_EQ(rows.begin(), rows.end());
}

TEST(arrow_columnar_rows, null_batch_zero_field_schema) {
    auto stream = build_arrow_ipc_stream_no_batch();
    sidecar::ArrowColumnarRows rows(as_span(stream));
    EXPECT_EQ(rows.size(), 0u);
    EXPECT_TRUE(rows.mapKeys().empty());
    EXPECT_EQ(rows.begin(), rows.end());
}

TEST(arrow_columnar_rows, date32_column_throws) {
    auto stream = build_arrow_ipc_stream({"day"}, {date32_array({0, 1})});
    EXPECT_THROW(sidecar::ArrowColumnarRows rows(as_span(stream)), std::runtime_error);
}

// --- Full pipeline: deserialize_and_match_columnar with format=arrow ---

TEST(event_bridge_arrow, arrow_input_msgpack_output_matches_and_decodes) {
    std::vector<sidecar::attribute_def> defs = {{"value", sidecar::attribute_type::integer}};
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    uint64_t id = mgr.subscribe("value > 10", "client-1");
    auto snap = mgr.snapshot();
    ASSERT_TRUE(snap && snap->tree);

    // Row 0 (value=5) doesn't match, rows 1 and 2 (42, 100) do - same shape as the equivalent
    // zerialize-format columnar test in test_event_bridge.cpp.
    auto stream = build_arrow_ipc_stream({"value"}, {int64_array({5, 42, 100})});

    std::size_t rows_searched = 0;
    auto result = sidecar::deserialize_and_match_columnar(
        *snap->tree, schema, sidecar::binary_format::arrow, as_span(stream), make_log(),
        nullptr, &rows_searched, sidecar::binary_format::msgpack);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(rows_searched, 3u);
    ASSERT_EQ(result->size(), 2u);
    EXPECT_TRUE(contains((*result)[0].matched_ids, id));
    EXPECT_TRUE(contains((*result)[1].matched_ids, id));

    zerialize::MsgPack::Deserializer row0(std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>((*result)[0].payload.data()), (*result)[0].payload.size()));
    zerialize::MsgPack::Deserializer row1(std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>((*result)[1].payload.data()), (*result)[1].payload.size()));
    EXPECT_EQ(row0["value"].asInt64(), 42);
    EXPECT_EQ(row1["value"].asInt64(), 100);
}

TEST(event_bridge_arrow, arrow_input_without_output_format_returns_nullopt) {
    std::vector<sidecar::attribute_def> defs = {{"value", sidecar::attribute_type::integer}};
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    mgr.subscribe("value > 10", "client-1");
    auto snap = mgr.snapshot();
    ASSERT_TRUE(snap && snap->tree);

    auto stream = build_arrow_ipc_stream({"value"}, {int64_array({42})});
    // output_format defaults to nullopt - reachable in practice only if
    // finalize_and_validate_config's own guard is bypassed; must still fail gracefully here.
    auto result = sidecar::deserialize_and_match_columnar(
        *snap->tree, schema, sidecar::binary_format::arrow, as_span(stream), make_log());
    EXPECT_FALSE(result.has_value());
}

TEST(event_bridge_arrow, arrow_as_output_format_returns_nullopt) {
    std::vector<sidecar::attribute_def> defs = {{"value", sidecar::attribute_type::integer}};
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    mgr.subscribe("value > 10", "client-1");
    auto snap = mgr.snapshot();
    ASSERT_TRUE(snap && snap->tree);

    auto stream = build_arrow_ipc_stream({"value"}, {int64_array({42})});
    auto result = sidecar::deserialize_and_match_columnar(
        *snap->tree, schema, sidecar::binary_format::arrow, as_span(stream), make_log(),
        nullptr, nullptr, sidecar::binary_format::arrow);
    EXPECT_FALSE(result.has_value());
}

TEST(event_bridge_arrow, arrow_input_with_date32_column_returns_nullopt) {
    std::vector<sidecar::attribute_def> defs = {{"value", sidecar::attribute_type::integer}};
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    mgr.subscribe("value > 10", "client-1");
    auto snap = mgr.snapshot();
    ASSERT_TRUE(snap && snap->tree);

    auto stream = build_arrow_ipc_stream({"day"}, {date32_array({0, 1})});
    auto result = sidecar::deserialize_and_match_columnar(
        *snap->tree, schema, sidecar::binary_format::arrow, as_span(stream), make_log(),
        nullptr, nullptr, sidecar::binary_format::msgpack);
    EXPECT_FALSE(result.has_value());
}

TEST(event_bridge_arrow, row_mode_rejects_arrow_format) {
    std::vector<sidecar::attribute_def> defs = {{"value", sidecar::attribute_type::integer}};
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    mgr.subscribe("value > 10", "client-1");
    auto snap = mgr.snapshot();
    ASSERT_TRUE(snap && snap->tree);

    auto stream = build_arrow_ipc_stream({"value"}, {int64_array({42})});
    auto result = sidecar::deserialize_and_match(
        *snap->tree, schema, sidecar::binary_format::arrow, as_span(stream), make_log());
    EXPECT_FALSE(result.has_value());
}
