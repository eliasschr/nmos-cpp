#include <atomic>
#include <chrono>
#include <thread>
#include <boost/asio.hpp>
#include <boost/mqtt5/mqtt_client.hpp>
#include "events_mqtt_sender.h"
#include "cpprest/asyncrt_utils.h"
#include "nmos/json_fields.h"
#include "nmos/slog.h"

namespace impl
{
    namespace
    {
        std::string utf8(const utility::string_t& value)
        {
            return utility::conversions::to_utf8string(value);
        }

        std::string connection_status(bool active)
        {
            return active ? R"({"message_type":"connection_status","active":true})"
                          : R"({"message_type":"connection_status","active":false})";
        }
    }

    struct events_mqtt_sender::impl
    {
        struct connection_logger
        {
            impl* owner;

            void at_connack(boost::mqtt5::reason_code reason, bool, const boost::mqtt5::connack_props&);
            void at_disconnect(boost::mqtt5::reason_code, const boost::mqtt5::disconnect_props&);
            void at_transport_error(boost::mqtt5::error_code);
        };

        using client_type = boost::mqtt5::mqtt_client<boost::asio::ip::tcp::socket, std::monostate, connection_logger>;

        boost::asio::io_context io;
        client_type client;
        boost::asio::steady_timer shutdown_timer;
        std::thread thread;
        std::atomic<bool> stopping{ false };
        // The following fields are accessed only on io's executor.
        bool connected{ false };
        bool disconnect_started{ false };
        bool ever_connected{ false };
        bool outage_reported{ false };
        bool connack_failure_reported{ false };
        // The registry destroys every sender before node_implementation_thread
        // returns, while this gate is still alive.
        slog::base_gate& gate;
        std::string sender_id;
        std::string broker_host;
        uint16_t broker_port;
        std::string broker_topic;
        std::string status_topic;
        std::string latest_state;

        impl(const nmos::id& sender_id_, const web::json::value& transport_params, const web::json::value& state, slog::base_gate& gate_)
            : client(io, {}, connection_logger{ this })
            , shutdown_timer(io)
            , gate(gate_)
            , sender_id(utf8(sender_id_))
        {
            // The event thread's JSON value cannot outlive this call, so retain
            // its serialized representation before starting MQTT connection handling.
            latest_state = utf8(state.serialize());

            const auto& params = transport_params.at(0);
            broker_host = utf8(nmos::fields::destination_host(params).as_string());
            broker_port = static_cast<uint16_t>(nmos::fields::destination_port(params).as_integer());
            broker_topic = utf8(nmos::fields::broker_topic(params).as_string());
            status_topic = utf8(nmos::fields::connection_status_broker_topic(params).as_string());

            client.credentials(sender_id);
            client.brokers(broker_host, broker_port);
            client.will(boost::mqtt5::will{ status_topic, connection_status(false), boost::mqtt5::qos_e::exactly_once, boost::mqtt5::retain_e::yes });
            client.async_run([](boost::mqtt5::error_code) {});

            thread = std::thread([this] { io.run(); });
        }

        ~impl()
        {
            stop();
            if (thread.joinable()) thread.join();
        }

        void stop()
        {
            if (stopping.exchange(true)) return;
            boost::asio::post(io, [this]
            {
                slog::log<slog::severities::info>(gate, SLOG_FLF) << "Stopping MQTT IS-07 sender " << sender_id << " and disconnecting from broker " << broker_host << ":" << broker_port;
                if (!connected)
                {
                    // Do not let Boost.MQTT5 queue a graceful-shutdown status
                    // while it is reconnecting to an unavailable broker.
                    begin_disconnect();
                    return;
                }

                connected = false;
                publish_retained(status_topic, connection_status(false), [this]
                {
                    shutdown_timer.cancel();
                    begin_disconnect();
                });

                // Permit the retained graceful-shutdown status to complete before
                // starting disconnect. async_disconnect may still take up to about
                // five seconds when the broker is unavailable.
                shutdown_timer.expires_after(std::chrono::seconds(1));
                shutdown_timer.async_wait([this](boost::mqtt5::error_code error)
                {
                    if (!error) begin_disconnect();
                });
            });
        }

        void publish_state(const web::json::value& state)
        {
            // The event thread's JSON value cannot outlive this call, so retain
            // its serialized representation before posting to the MQTT context.
            auto serialized_state = utf8(state.serialize());
            if (stopping.load()) return;

            boost::asio::post(io, [this, serialized_state = std::move(serialized_state)]
            {
                if (stopping.load()) return;

                latest_state = std::move(serialized_state);
                if (!connected) return;

                publish_retained(broker_topic, latest_state, [] {});
            });
        }

        void on_connack(boost::mqtt5::reason_code reason)
        {
            // A successful MQTT v5 CONNACK has reason code 0. Failed CONNACKs
            // are followed by Boost.MQTT5's built-in reconnect handling.
            if (stopping.load()) return;
            if (reason.value() != boost::mqtt5::reason_codes::success.value())
            {
                if (!connack_failure_reported)
                {
                    slog::log<slog::severities::warning>(gate, SLOG_FLF) << "MQTT IS-07 sender " << sender_id << " received failed CONNACK from broker " << broker_host << ":" << broker_port << ": " << reason << " [" << static_cast<unsigned>(reason.value()) << "]";
                    connack_failure_reported = true;
                }
                outage_reported = true;
                return;
            }

            connected = true;
            if (!ever_connected)
                slog::log<slog::severities::info>(gate, SLOG_FLF) << "MQTT IS-07 sender " << sender_id << " connected to broker " << broker_host << ":" << broker_port;
            else if (outage_reported)
                slog::log<slog::severities::info>(gate, SLOG_FLF) << "MQTT IS-07 sender " << sender_id << " reconnected to broker " << broker_host << ":" << broker_port;
            ever_connected = true;
            outage_reported = false;
            connack_failure_reported = false;

            // Do not start QoS2 publishes from Boost.MQTT5's connection
            // logger callback. Preserve their ordering when the connection is
            // still valid on the next executor turn.
            boost::asio::post(io, [this]
            {
                if (stopping.load() || !connected) return;

                publish_retained(status_topic, connection_status(true), [] {});
                publish_retained(broker_topic, latest_state, [] {});
            });
        }

        void on_disconnect(boost::mqtt5::reason_code reason)
        {
            connected = false;
            if (stopping.load() || outage_reported) return;

            slog::log<slog::severities::warning>(gate, SLOG_FLF) << "MQTT IS-07 sender " << sender_id << " was disconnected by broker " << broker_host << ":" << broker_port << ": " << reason << " [" << static_cast<unsigned>(reason.value()) << "]";
            outage_reported = true;
        }

        void on_transport_error(boost::mqtt5::error_code error)
        {
            connected = false;
            if (stopping.load() || outage_reported) return;

            slog::log<slog::severities::warning>(gate, SLOG_FLF) << "MQTT IS-07 sender " << sender_id << " lost transport to broker " << broker_host << ":" << broker_port << ": " << error.message() << " [" << error << "]";
            outage_reported = true;
        }

        template <typename Handler>
        void publish_retained(const std::string& topic, const std::string& payload, Handler&& handler)
        {
            const boost::mqtt5::publish_props props{};
            client.async_publish<boost::mqtt5::qos_e::exactly_once>(topic, payload, boost::mqtt5::retain_e::yes, props,
                [handler = std::forward<Handler>(handler)](boost::mqtt5::error_code, auto, auto) mutable
                {
                    handler();
                });
        }

        void begin_disconnect()
        {
            if (disconnect_started) return;
            disconnect_started = true;
            client.async_disconnect([](boost::mqtt5::error_code) {});
        }
    };

    void events_mqtt_sender::impl::connection_logger::at_connack(boost::mqtt5::reason_code reason, bool, const boost::mqtt5::connack_props&)
    {
        owner->on_connack(reason);
    }

    void events_mqtt_sender::impl::connection_logger::at_disconnect(boost::mqtt5::reason_code reason, const boost::mqtt5::disconnect_props&)
    {
        owner->on_disconnect(reason);
    }

    void events_mqtt_sender::impl::connection_logger::at_transport_error(boost::mqtt5::error_code error)
    {
        owner->on_transport_error(error);
    }

    events_mqtt_sender::events_mqtt_sender(const nmos::id& sender_id, const web::json::value& transport_params, const web::json::value& state, slog::base_gate& gate)
        : impl_(new impl(sender_id, transport_params, state, gate))
    {}

    events_mqtt_sender::~events_mqtt_sender() = default;

    void events_mqtt_sender::publish_state(const web::json::value& state)
    {
        impl_->publish_state(state);
    }

    void events_mqtt_sender::stop()
    {
        impl_->stop();
    }
}
