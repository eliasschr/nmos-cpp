#ifndef NMOS_CPP_NODE_EVENTS_MQTT_SENDER_H
#define NMOS_CPP_NODE_EVENTS_MQTT_SENDER_H

#include <memory>
#include "cpprest/json.h"
#include "nmos/id.h"

namespace slog
{
    class base_gate;
}

namespace impl
{
    // Deliberately keeps Boost.MQTT5 out of the example-node interface.
    class events_mqtt_sender
    {
    public:
        events_mqtt_sender(const nmos::id& sender_id, const web::json::value& transport_params, const web::json::value& state, slog::base_gate& gate);
        ~events_mqtt_sender();

        events_mqtt_sender(const events_mqtt_sender&) = delete;
        events_mqtt_sender& operator=(const events_mqtt_sender&) = delete;

        void publish_state(const web::json::value& state);
        void stop();

    private:
        struct impl;
        std::unique_ptr<impl> impl_;
    };
}

#endif
