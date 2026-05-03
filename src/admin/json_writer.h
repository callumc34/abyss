#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace abyss::admin::internal {

// Streaming JSON writer for the /status payload. Tracks object/array nesting
// and inserts commas between siblings. Not a full JSON library — supports
// only the shapes we render. RFC 8259-compliant string escaping.
class JsonWriter {
 public:
  JsonWriter();

  // Object scope: { ... }. Members are added via Key() then a value method.
  void BeginObject();
  void EndObject();

  // Array scope: [ ... ]. Elements are added via value methods directly.
  void BeginArray();
  void EndArray();

  // Member key inside the current object. Must be followed by exactly one
  // value method.
  void Key(std::string_view key);

  void Null();
  void Bool(bool value);
  void Int(int64_t value);
  void UInt(uint64_t value);
  void String(std::string_view value);

  // Returns the rendered document. Must be called at top level (no open
  // scopes).
  std::string Finish();

 private:
  enum class Scope : uint8_t { kRoot, kObject, kArray };

  void OnValueWritten();
  void EmitSeparator();
  void EmitEscapedString(std::string_view value);

  std::string out_;
  std::vector<Scope> scopes_;
  std::vector<bool> first_in_scope_;
  bool expects_value_ = false;
};

}  // namespace abyss::admin::internal
