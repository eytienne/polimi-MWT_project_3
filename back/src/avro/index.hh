#pragma once

#include <variant>

#include "Calculus.hh"
#include "CalculusResult.hh"
#include "ErrorResult.hh"
#include "TextFormatting.hh"
#include "TextFormattingResult.hh"
#include "ImageCompression.hh"
#include "ImageCompressionResult.hh"

namespace c {
    using Task = std::variant<c::Calculus, c::ImageCompression, c::TextFormatting>;
    using TaskResult = std::variant<c::CalculusResult, c::ImageCompressionResult, c::TextFormattingResult>;
}

namespace avro {
	template <typename... TArgs>
	struct codec_traits<std::variant<TArgs...>> {
		static void encode(Encoder& e, const std::variant<TArgs...>& v) {
			std::visit([&e](auto &&arg) { avro::encode(e, arg); }, v);
		}
		static void decode(Decoder& d, std::variant<TArgs...>& v) {
			std::visit([&d](auto &&arg) { avro::decode(d, arg); }, v);
		}
	};
}