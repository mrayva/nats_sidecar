#pragma once

// ArrowColumnarRows - reads Apache Arrow IPC columnar batches (as produced by the pg_arrow
// Postgres extension's rows_to_arrow(anyarray) -> bytea, always exactly one RecordBatch per
// call - see pg_arrow.cpp's own file-level comment) and exposes them via zerialize's duck-typed
// Reader concept (zerialize/include/zerialize/concepts.hpp), so they plug into the SAME generic
// matching pipeline (populate_event/match_message/match_columnar_batch, event_bridge.hpp) as
// every other columnar format - with ZERO changes to zerialize itself. This works because
// zerialize::Reader<V> is a pure C++20 structural concept (isNull/isInt/.../asInt64/.../
// mapKeys()/operator[](string_view)/operator[](size_t), no base class or vtable) - any type
// with the right member functions satisfies it, confirmed by reading concepts.hpp directly
// before writing this.
//
// Unlike zerialize's own ColumnarRows<V> (zerialize/include/zerialize/columnar.hpp), which
// caches and advances a "current row" because most zerialize formats can only iterate
// sequentially, Arrow arrays support true O(1) random access (array->Value(i),
// array->IsNull(i)) - so this adapter just tracks a row index and looks up values on demand in
// operator[](key), with no caching/advance machinery needed. It otherwise mirrors
// ColumnarRows<V>'s own shape closely (one object plays both "the whole batch" and "the current
// row" simultaneously, non-const iteration mutating internal row state, every ValueView
// accessor that doesn't apply to a map-typed row stubbed as [[noreturn]]) since that's exactly
// the same role match_columnar_batch (event_bridge.cpp) already needs from either type.
//
// LIFETIME: the arrow::Buffer this wraps is deliberately non-owning - it aliases the caller's
// own payload bytes directly, exactly like pg_arrow.cpp's own arrow_to_jsonb() does (confirmed
// by reading that function before writing this), rather than copying them. This
// ArrowColumnarRows, and every row/cell view it hands out, is only valid for as long as the
// `payload` span passed to the constructor stays alive. Safe by construction as long as an
// ArrowColumnarRows never outlives the call frame it's constructed in - exactly like the
// existing `typename Protocol::Deserializer reader(bytes)` locals in event_bridge.cpp's
// match_columnar_batch, which never escape that function either. Do not store one, or a
// row/cell view derived from one, anywhere that outlives the payload buffer.
//
// TYPE MAPPING (Arrow -> sidecar::attribute_type) and its deliberate v1 limits - see the
// project README for the full table and rationale:
//   int16/int32/int64 -> integer;  float32/float64/half_float -> float;  boolean -> boolean
//   utf8 -> string (asStringView() views Arrow's own buffer directly, zero-copy)
//   binary -> string, as a raw byte reinterpretation of GetView() - NOT the base64-tagged form
//     pg_arrow's own arrow_to_jsonb() uses for this same Arrow type, since attribute_type has
//     no blob/binary kind at all. A deliberate choice, not an oversight.
//   half_float -> float, via arrow::util::Float16::FromBits(...).ToDouble() - every float16
//     value is exactly representable as a double (fewer exponent/mantissa bits either way), so
//     this is a lossless widening, exactly like float32/float64 already are - no precision
//     concern at all, unlike the decimal cases below.
//   decimal32/decimal64/decimal128/decimal256 -> attribute_type::decimal, pstree-only (a-tree/
//     be-tree throw - see matching_engine.hpp's event_sink::with_decimal). All four widths
//     widen to arrow::Decimal256 (BasicDecimal256's own explicit widening constructors from
//     128/64/32, confirmed against the installed Arrow headers before relying on them) and
//     Rescale() to this attribute's own canonical decimal_scale (attribute_schema, event_bridge.
//     hpp) - the ONE scale every value reaching this dimension is rescaled to, event or literal
//     alike (see pstree/pst_dynamic.hpp's AttrSchema::decimalScale for why scale lives once per
//     attribute, never per value). The resulting Int256 (pstree/int256.hpp, four uint64_t limbs,
//     no scale/precision tag) is exact for the event side; the query/literal side stays capped
//     at IEEE-754 double precision regardless (be-tree's own reused parser has no decimal
//     literal kind at all - ast_compare_value_e is int64/double only - so a subscription's own
//     threshold is already approximated before nats_sidecar ever sees it, the same as every
//     other numeric attribute type already accepts). This superseded an earlier same-session
//     stopgap that mapped decimal32/64/128 to `string` (exact text, but ordering comparisons
//     against a string-typed attribute are a hard PARSE-TIME REJECTION in this grammar, not a
//     silently-wrong lexicographic one - only `=`/`!=`/`in`/`not in` were ever reachable) - kept
//     only as historical context, not the current behavior.
//   date32 / timestamp -> UNSUPPORTED. Rejected at construction time (the whole batch is
//     poisoned - propagates as a thrown exception, caught by deserialize_and_match_columnar's
//     existing try/catch, exactly like a malformed payload in any other format). Deliberate:
//     attribute_type has no dedicated timestamp kind, and silently reinterpreting date32(days)
//     and timestamp(micros) as the same `integer` would be a real correctness footgun (a
//     subscription author's threshold comparison could be silently wrong with no type signal
//     anywhere to catch the mistake). Cheap to add later with a real epoch/unit convention once
//     there's a concrete need.
//   Nothing else ever occurs - pg_arrow rejects arrays/composites/every other PostgreSQL type
//   at the SQL layer, so no other Arrow type can appear in a real rows_to_arrow() payload.
//   string_list/integer_list attributes are simply unreachable via Arrow input as a result -
//   not an error case, just never populated.

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/decimal.h>
#include <arrow/util/float16.h>
#include <pstree/int256.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace sidecar {

namespace arrow_detail {

[[noreturn]] inline void unsupported(const char* accessor) {
    throw std::runtime_error(std::string("ArrowColumnarRows: ") + accessor + " not applicable here");
}

inline bool is_supported_type(arrow::Type::type t) {
    switch (t) {
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::INT64:
        case arrow::Type::FLOAT:
        case arrow::Type::DOUBLE:
        case arrow::Type::HALF_FLOAT:
        case arrow::Type::BOOL:
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
        case arrow::Type::DECIMAL32:
        case arrow::Type::DECIMAL64:
        case arrow::Type::DECIMAL128:
        case arrow::Type::DECIMAL256:
            return true;
        default:
            return false;
    }
}

} // namespace arrow_detail

// One cell: a single (column, row) value. Satisfies zerialize::ValueView (concepts.hpp) only -
// not the full Reader concept - since ValueView alone is everything a scalar cell needs to
// provide; Reader additionally requires operator[], only needed by the row-cursor type below.
class ArrowCellView {
public:
    ArrowCellView() = default;
    // type_id() is a pure function of `array` alone (never `row`) - cached here ONCE instead of
    // re-derived by every accessor below. Found via `perf annotate` on a real N=12 fleet run
    // under real load (not a synthetic benchmark): array_->type_id() involves a real, measurable
    // shared_ptr<ArrayData>/shared_ptr<DataType> chase through Arrow's own internal object graph
    // (arrow::ArrayData, arrow::DataType) - `populate_event()`'s generic dispatch (event_bridge.hpp)
    // calls an isXxx() check THEN the matching asXxx() accessor for every cell (e.g. isFloat()
    // then asDouble() for a float_val attribute), each independently re-deriving type_id() -
    // exactly 2x the necessary work per cell for every attribute type actually reachable from
    // populate_event's own dispatch shape. Caching removes that redundancy entirely - array_'s
    // type can never change mid-lifetime of one ArrowCellView, so this is a pure perf fix with
    // no behavior change.
    ArrowCellView(const arrow::Array* array, std::int64_t row)
        : array_(array), row_(row), type_(array != nullptr ? array->type_id() : arrow::Type::NA) {}

    bool isNull() const { return array_ == nullptr || array_->IsNull(row_); }
    bool isBool() const { return !isNull() && type_ == arrow::Type::BOOL; }
    bool isInt() const {
        if (isNull()) return false;
        return type_ == arrow::Type::INT16 || type_ == arrow::Type::INT32 || type_ == arrow::Type::INT64;
    }
    bool isUInt() const { return false; } // pg_arrow never emits an unsigned Arrow type
    bool isFloat() const {
        if (isNull()) return false;
        return type_ == arrow::Type::FLOAT || type_ == arrow::Type::DOUBLE
            || type_ == arrow::Type::HALF_FLOAT;
    }
    bool isString() const {
        if (isNull()) return false;
        return type_ == arrow::Type::STRING || type_ == arrow::Type::BINARY;
    }
    bool isBlob()  const { return false; } // binary maps to string, not blob - see this file's own header comment
    bool isMap()   const { return false; }
    bool isArray() const { return false; }
    // Not part of zerialize::ValueView's own structural concept (no other protocol has a native
    // decimal kind - see this file's own header comment) - event_bridge.hpp's populate_event()
    // gates its call to this and asDecimal() behind `if constexpr (requires {...})`, so every
    // other Reader type simply never instantiates either.
    bool isDecimal() const {
        if (isNull()) return false;
        return type_ == arrow::Type::DECIMAL32 || type_ == arrow::Type::DECIMAL64
            || type_ == arrow::Type::DECIMAL128 || type_ == arrow::Type::DECIMAL256;
    }

    std::int8_t  asInt8()  const { return static_cast<std::int8_t>(asInt64()); }
    std::int16_t asInt16() const { return static_cast<std::int16_t>(asInt64()); }
    std::int32_t asInt32() const { return static_cast<std::int32_t>(asInt64()); }
    std::int64_t asInt64() const {
        switch (type_) {
            case arrow::Type::INT16: return static_cast<const arrow::Int16Array&>(*array_).Value(row_);
            case arrow::Type::INT32: return static_cast<const arrow::Int32Array&>(*array_).Value(row_);
            case arrow::Type::INT64: return static_cast<const arrow::Int64Array&>(*array_).Value(row_);
            default: arrow_detail::unsupported("asInt64");
        }
    }
    [[noreturn]] std::uint8_t  asUInt8()  const { arrow_detail::unsupported("asUInt8"); }
    [[noreturn]] std::uint16_t asUInt16() const { arrow_detail::unsupported("asUInt16"); }
    [[noreturn]] std::uint32_t asUInt32() const { arrow_detail::unsupported("asUInt32"); }
    [[noreturn]] std::uint64_t asUInt64() const { arrow_detail::unsupported("asUInt64"); }
    float  asFloat()  const { return static_cast<float>(asDouble()); }
    double asDouble() const {
        switch (type_) {
            case arrow::Type::FLOAT:  return static_cast<const arrow::FloatArray&>(*array_).Value(row_);
            case arrow::Type::DOUBLE: return static_cast<const arrow::DoubleArray&>(*array_).Value(row_);
            // Lossless: every float16 value is exactly representable as a double (fewer
            // exponent/mantissa bits either way) - same "widen, never lose precision" rule
            // float32/float64 already get, unlike the decimal cases below.
            case arrow::Type::HALF_FLOAT:
                return arrow::util::Float16::FromBits(
                    static_cast<const arrow::HalfFloatArray&>(*array_).Value(row_)).ToDouble();
            default: arrow_detail::unsupported("asDouble");
        }
    }
    std::string asString() const { return std::string(asStringView()); }
    std::string_view asStringView() const {
        switch (type_) {
            case arrow::Type::STRING: {
                auto v = static_cast<const arrow::StringArray&>(*array_).GetView(row_);
                return std::string_view(v.data(), v.size());
            }
            case arrow::Type::BINARY: {
                auto v = static_cast<const arrow::BinaryArray&>(*array_).GetView(row_);
                return std::string_view(v.data(), v.size());
            }
            default:
                arrow_detail::unsupported("asStringView");
        }
    }
    bool asBool() const { return static_cast<const arrow::BooleanArray&>(*array_).Value(row_); }

    // Reads this cell's raw decimal bytes (whichever of DECIMAL32/64/128/256 the column actually
    // is), widens to arrow::Decimal256 via its own explicit widening constructors from
    // 128/64/32 (confirmed against the installed Arrow headers before relying on them), and
    // Rescale()s from the COLUMN's own declared scale to `targetScale` (this attribute's one
    // canonical decimal_scale - see this file's own header comment). Throws on a genuine
    // out-of-range rescale (a value that doesn't fit once rescaled), matching pg_arrow's own
    // decimalN_from_numeric() error-handling convention for the identical situation on the
    // publish side. Returns a bare pstree::Int256 - no scale/precision travels with it, by
    // design (AttrSchema::decimalScale's own comment explains why).
    pstree::Int256 asDecimal(std::int32_t targetScale) const {
        int32_t columnScale = static_cast<const arrow::DecimalType&>(*array_->type()).scale();
        arrow::Decimal256 wide;
        switch (type_) {
            case arrow::Type::DECIMAL32:
                wide = arrow::Decimal256(arrow::Decimal32(
                    static_cast<const arrow::Decimal32Array&>(*array_).Value(row_)));
                break;
            case arrow::Type::DECIMAL64:
                wide = arrow::Decimal256(arrow::Decimal64(
                    static_cast<const arrow::Decimal64Array&>(*array_).Value(row_)));
                break;
            case arrow::Type::DECIMAL128:
                wide = arrow::Decimal256(arrow::Decimal128(
                    static_cast<const arrow::Decimal128Array&>(*array_).Value(row_)));
                break;
            case arrow::Type::DECIMAL256:
                wide = arrow::Decimal256(
                    static_cast<const arrow::Decimal256Array&>(*array_).Value(row_));
                break;
            default:
                arrow_detail::unsupported("asDecimal");
        }
        if (columnScale != targetScale) {
            auto rescaled = wide.Rescale(columnScale, targetScale);
            if (!rescaled.ok()) {
                throw std::runtime_error(
                    "ArrowColumnarRows: decimal value does not fit after rescaling from "
                    "column scale " + std::to_string(columnScale) + " to attribute scale " +
                    std::to_string(targetScale) + ": " + rescaled.status().ToString());
            }
            wide = rescaled.ValueOrDie();
        }
        pstree::Int256 out;
        out.limb = wide.little_endian_array();
        return out;
    }

    [[noreturn]] std::span<const std::string_view> mapKeys() const { arrow_detail::unsupported("mapKeys"); }
    bool contains(std::string_view) const { return false; }
    [[noreturn]] std::size_t arraySize() const { arrow_detail::unsupported("arraySize"); }
    [[noreturn]] std::span<const std::byte> asBlob() const { arrow_detail::unsupported("asBlob"); }

    // zerialize::write_value() (translate.hpp) recurses into a map/array value's own elements
    // via operator[] unconditionally - the recursive calls are only ever reached at runtime
    // behind an isMap()/isArray() check (both always false here, a cell is always a scalar),
    // but the expression still has to compile for every V it might be instantiated with. Not
    // part of the ValueView concept a scalar cell strictly needs (Reader's operator[] is what
    // needs this), added purely so this type instantiates cleanly as write_value's recursion
    // target - never actually invoked.
    [[noreturn]] ArrowCellView operator[](std::string_view) const { arrow_detail::unsupported("operator[](string_view)"); }
    [[noreturn]] ArrowCellView operator[](std::size_t) const { arrow_detail::unsupported("operator[](size_t)"); }

private:
    const arrow::Array* array_ = nullptr;
    std::int64_t row_ = 0;
    arrow::Type::type type_ = arrow::Type::NA;
};

// The row cursor: satisfies zerialize::Reader (ValueView + operator[]). One ArrowColumnarRows
// represents a whole batch AND the "current row" simultaneously - mirrors zerialize's own
// ColumnarRows<V>'s role (columnar.hpp), which the same generic code (match_columnar_batch,
// populate_event) already treats this way for every other columnar format.
class ArrowColumnarRows {
public:
    // `payload` must outlive this object and everything it hands out - see this file's own
    // header comment.
    explicit ArrowColumnarRows(std::span<const char> payload) {
        auto buffer = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t*>(payload.data()), static_cast<int64_t>(payload.size()));
        auto stream = std::make_shared<arrow::io::BufferReader>(buffer);
        auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(stream);
        if (!reader_result.ok()) {
            throw std::runtime_error(
                "ArrowColumnarRows: failed to open IPC stream: " + reader_result.status().ToString());
        }
        auto reader = *reader_result;
        auto status = reader->ReadNext(&batch_);
        if (!status.ok()) {
            throw std::runtime_error(
                "ArrowColumnarRows: failed to read RecordBatch: " + status.ToString());
        }
        // A null batch here is a real, documented case, not an error - a zero-field schema
        // yields no RecordBatch at all from ReadNext(), even on success (see pg_arrow.cpp's own
        // arrow_to_jsonb() for the identical check). Treated as a valid, empty (0 rows, 0
        // columns) batch: n_/keys_/key_views_/columns_ all stay at their default-empty state.
        if (!batch_) return;

        n_ = batch_->num_rows();
        const int num_columns = batch_->num_columns();
        keys_.reserve(static_cast<std::size_t>(num_columns));
        columns_.reserve(static_cast<std::size_t>(num_columns));
        for (int i = 0; i < num_columns; ++i) {
            const arrow::Array* column = batch_->column(i).get();
            if (!arrow_detail::is_supported_type(column->type_id())) {
                throw std::runtime_error(
                    "ArrowColumnarRows: column \"" + batch_->column_name(i) +
                    "\" has unsupported Arrow type \"" + column->type()->ToString() +
                    "\" (date/timestamp columns are not supported - see this file's own "
                    "header comment)");
            }
            keys_.emplace_back(batch_->column_name(i));
            columns_.push_back(column);
        }
        key_views_.reserve(keys_.size());
        for (const auto& k : keys_) key_views_.push_back(k);
    }

    std::size_t size() const { return static_cast<std::size_t>(n_); }

    // ---- "current row" Reader surface - mirrors ColumnarRows<V>'s own shape exactly ----
    bool isNull()   const { return false; }
    bool isBool()   const { return false; }
    bool isInt()    const { return false; }
    bool isUInt()   const { return false; }
    bool isFloat()  const { return false; }
    bool isString() const { return false; }
    bool isBlob()   const { return false; }
    bool isMap()    const { return true; }
    bool isArray()  const { return false; }

    [[noreturn]] std::int8_t      asInt8()       const { arrow_detail::unsupported("asInt8"); }
    [[noreturn]] std::int16_t     asInt16()      const { arrow_detail::unsupported("asInt16"); }
    [[noreturn]] std::int32_t     asInt32()      const { arrow_detail::unsupported("asInt32"); }
    [[noreturn]] std::int64_t     asInt64()      const { arrow_detail::unsupported("asInt64"); }
    [[noreturn]] std::uint8_t     asUInt8()      const { arrow_detail::unsupported("asUInt8"); }
    [[noreturn]] std::uint16_t    asUInt16()     const { arrow_detail::unsupported("asUInt16"); }
    [[noreturn]] std::uint32_t    asUInt32()     const { arrow_detail::unsupported("asUInt32"); }
    [[noreturn]] std::uint64_t    asUInt64()     const { arrow_detail::unsupported("asUInt64"); }
    [[noreturn]] float            asFloat()      const { arrow_detail::unsupported("asFloat"); }
    [[noreturn]] double           asDouble()     const { arrow_detail::unsupported("asDouble"); }
    [[noreturn]] std::string      asString()     const { arrow_detail::unsupported("asString"); }
    [[noreturn]] std::string_view asStringView() const { arrow_detail::unsupported("asStringView"); }
    [[noreturn]] bool             asBool()       const { arrow_detail::unsupported("asBool"); }
    [[noreturn]] std::span<const std::byte> asBlob() const { arrow_detail::unsupported("asBlob"); }

    [[noreturn]] std::size_t arraySize() const { arrow_detail::unsupported("arraySize"); }
    [[noreturn]] ArrowCellView operator[](std::size_t) const { arrow_detail::unsupported("operator[](size_t)"); }

    std::span<const std::string_view> mapKeys() const { return key_views_; }
    bool contains(std::string_view key) const { return find_index(key).has_value(); }

    ArrowCellView operator[](std::string_view key) const {
        auto idx = find_index(key);
        if (!idx) {
            throw std::runtime_error("ArrowColumnarRows: no such column \"" + std::string(key) + "\"");
        }
        return ArrowCellView(columns_[*idx], row_);
    }

    // ---- forward, single-pass iteration over rows - just advances an index, no per-row
    // caching needed (see this file's own header comment on why, unlike ColumnarRows) ----
    struct iterator {
        using iterator_category = std::input_iterator_tag;
        using value_type        = ArrowColumnarRows;
        using difference_type   = std::ptrdiff_t;
        using reference         = ArrowColumnarRows&;
        using pointer           = ArrowColumnarRows*;

        ArrowColumnarRows* self = nullptr;
        std::int64_t pos = 0;

        reference operator*() const { return *self; }
        iterator& operator++() { ++pos; self->row_ = pos; return *this; }
        void operator++(int) { ++(*this); }
        friend bool operator==(const iterator& a, const iterator& b) { return a.pos == b.pos; }
    };
    iterator begin() { row_ = 0; return iterator{this, 0}; }
    iterator end()   { return iterator{this, n_}; }

private:
    std::optional<std::size_t> find_index(std::string_view key) const {
        for (std::size_t i = 0; i < key_views_.size(); ++i) {
            if (key_views_[i] == key) return i;
        }
        return std::nullopt;
    }

    std::shared_ptr<arrow::RecordBatch> batch_;
    std::vector<std::string> keys_;
    std::vector<std::string_view> key_views_;
    std::vector<const arrow::Array*> columns_;
    std::int64_t n_ = 0;
    std::int64_t row_ = 0;
};

} // namespace sidecar
