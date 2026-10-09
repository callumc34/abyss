add_library(abyss_engine
  decide.cpp
  doorkeeper.cpp
  hot_replayer.cpp
  loader.cpp
  read_path.cpp
  sequencer.cpp
  tiering_engine.cpp
  recovery_coordinator.cpp
  bounded_thread_shard_scheduler.cpp
)
add_library(abyss::engine ALIAS abyss_engine)

target_link_libraries(abyss_engine
  PUBLIC abyss::core abyss::consumer abyss::hot abyss::queue abyss::metrics
  PRIVATE abyss::log
)
