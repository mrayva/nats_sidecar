#include "event_bridge.hpp"

namespace sidecar {

std::optional<std::vector<uint64_t>> deserialize_and_match(
    const atree::Tree& tree,
    const attribute_schema& schema,
    binary_format format,
    std::span<const char> payload,
    std::shared_ptr<spdlog::logger> log)
{
    try {
        auto bytes = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(payload.data()), payload.size());

        switch (format) {
            case binary_format::msgpack: {
                zerialize::MsgPack::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log);
            }
            case binary_format::cbor: {
                zerialize::CBOR::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log);
            }
            case binary_format::flexbuffers: {
                zerialize::Flex::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log);
            }
            case binary_format::zera: {
                zerialize::Zera::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log);
            }
        }
    } catch (const std::exception& e) {
        if (log) log->debug("event_bridge: deserialization failed: {}", e.what());
    }

    return std::nullopt;
}

} // namespace sidecar
