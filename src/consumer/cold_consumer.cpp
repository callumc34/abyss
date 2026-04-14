#include "abyss/consumer/cold_consumer.h"

namespace abyss::consumer {

ColdConsumer::ColdConsumer(core::Queue& queue, core::ColdStore& store, CompactionBuffer& buffer,
                           core::ShardId shard)
    : queue_(queue), store_(store), buffer_(buffer), shard_(shard) {}

void ColdConsumer::Start() {
  // Will spawn dedicated consumer thread
}

void ColdConsumer::Stop() {
  // Will signal thread to stop and join
}

}  // namespace abyss::consumer
