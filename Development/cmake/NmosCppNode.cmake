# nmos-cpp-node executable

set(NMOS_CPP_NODE_SOURCES
    nmos-cpp-node/main.cpp
    nmos-cpp-node/events_mqtt_sender.cpp
    nmos-cpp-node/node_implementation.cpp
    )
set(NMOS_CPP_NODE_HEADERS
    nmos-cpp-node/node_implementation.h
    nmos-cpp-node/events_mqtt_sender.h
    )

add_executable(
    nmos-cpp-node
    ${NMOS_CPP_NODE_SOURCES}
    ${NMOS_CPP_NODE_HEADERS}
    nmos-cpp-node/config.json
    )

source_group("Source Files" FILES ${NMOS_CPP_NODE_SOURCES})
source_group("Header Files" FILES ${NMOS_CPP_NODE_HEADERS})

target_link_libraries(
    nmos-cpp-node
    nmos-cpp::compile-settings
    nmos-cpp::nmos-cpp
    )
# Boost.MQTT5 is header-only but requires C++17. Keep that requirement local to
# the example executable rather than changing the nmos-cpp library baseline.
target_compile_features(nmos-cpp-node PRIVATE cxx_std_17)
# root directory to find e.g. nmos-cpp-node/node_implementation.h
target_include_directories(nmos-cpp-node PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    )

list(APPEND NMOS_CPP_TARGETS nmos-cpp-node)
