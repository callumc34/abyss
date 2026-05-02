#include "json_writer.h"

#include <array>
#include <cstdint>
#include <string>
#include <string_view>

namespace abyss::admin::internal {

JsonWriter::JsonWriter() {
  scopes_.push_back(Scope::kRoot);
  first_in_scope_.push_back(true);
}

void JsonWriter::BeginObject() {
  EmitSeparator();
  out_ += '{';
  scopes_.push_back(Scope::kObject);
  first_in_scope_.push_back(true);
  expects_value_ = false;
}

void JsonWriter::EndObject() {
  out_ += '}';
  scopes_.pop_back();
  first_in_scope_.pop_back();
  OnValueWritten();
}

void JsonWriter::BeginArray() {
  EmitSeparator();
  out_ += '[';
  scopes_.push_back(Scope::kArray);
  first_in_scope_.push_back(true);
  expects_value_ = false;
}

void JsonWriter::EndArray() {
  out_ += ']';
  scopes_.pop_back();
  first_in_scope_.pop_back();
  OnValueWritten();
}

void JsonWriter::Key(std::string_view key) {
  if (!first_in_scope_.back()) out_ += ',';
  first_in_scope_.back() = false;
  EmitEscapedString(key);
  out_ += ':';
  expects_value_ = true;
}

void JsonWriter::Null() {
  EmitSeparator();
  out_ += "null";
  OnValueWritten();
}

void JsonWriter::Bool(bool value) {
  EmitSeparator();
  out_ += value ? "true" : "false";
  OnValueWritten();
}

void JsonWriter::Int(int64_t value) {
  EmitSeparator();
  out_ += std::to_string(value);
  OnValueWritten();
}

void JsonWriter::UInt(uint64_t value) {
  EmitSeparator();
  out_ += std::to_string(value);
  OnValueWritten();
}

void JsonWriter::String(std::string_view value) {
  EmitSeparator();
  EmitEscapedString(value);
  OnValueWritten();
}

std::string JsonWriter::Finish() {
  std::string result;
  result.swap(out_);
  scopes_.clear();
  first_in_scope_.clear();
  scopes_.push_back(Scope::kRoot);
  first_in_scope_.push_back(true);
  expects_value_ = false;
  return result;
}

void JsonWriter::OnValueWritten() {
  if (scopes_.back() != Scope::kObject) {
    if (!first_in_scope_.empty()) first_in_scope_.back() = false;
  }
  expects_value_ = false;
}

void JsonWriter::EmitSeparator() {
  // Object-keyed values already have ':' from Key(); only array elements and
  // bare-root values need comma handling.
  if (expects_value_) return;
  if (scopes_.back() == Scope::kArray) {
    if (!first_in_scope_.back()) out_ += ',';
    first_in_scope_.back() = false;
  }
}

void JsonWriter::EmitEscapedString(std::string_view value) {
  out_ += '"';
  for (unsigned char c : value) {
    switch (c) {
      case '"':
        out_ += "\\\"";
        break;
      case '\\':
        out_ += "\\\\";
        break;
      case '\b':
        out_ += "\\b";
        break;
      case '\f':
        out_ += "\\f";
        break;
      case '\n':
        out_ += "\\n";
        break;
      case '\r':
        out_ += "\\r";
        break;
      case '\t':
        out_ += "\\t";
        break;
      default:
        if (c < 0x20) {
          static constexpr std::array<char, 16> kHex{'0', '1', '2', '3', '4', '5', '6', '7',
                                                     '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
          out_ += "\\u00";
          out_ += kHex.at((c >> 4U) & 0x0FU);
          out_ += kHex.at(c & 0x0FU);
        } else {
          out_ += static_cast<char>(c);
        }
        break;
    }
  }
  out_ += '"';
}

}  // namespace abyss::admin::internal
