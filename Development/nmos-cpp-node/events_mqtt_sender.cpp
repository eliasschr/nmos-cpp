#include <atomic>
#include <chrono>
#include <thread>
#include <boost/asio.hpp>
#include <boost/mqtt5/mqtt_client.hpp>
#include "events_mqtt_sender.h"
#include "cpprest/asyncrt_utils.h"
#include "nmos/json_fields.h"

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
        using client_type = boost::mqtt5::mqtt_client<boost::asio::ip::tcp::socket>;

        boost::asio::io_context io;
        client_type client{ io };
        boost::asio::steady_timer shutdown_timer{ io };
        std::thread thread;
        std::atomic<bool> stopping{ false };
        std::string status_topic;

        impl(const nmos::id& sender_id, const web::json::value& transport_params, const web::json::value& state)
        {
            const auto& params = transport_params.at(0);
            const auto broker_host = utf8(nmos::fields::destination_host(params).as_string());
            const auto broker_port = static_cast<uint16_t>(nmos::fields::destination_port(params).as_integer());
            const auto broker_topic = utf8(nmos::fields::broker_topic(params).as_string());
            status_topic = utf8(nmos::fields::connection_status_broker_topic(params).as_string());

            client.credentials(utf8(sender_id));
            client.brokers(broker_host, broker_port);
            client.will(boost::mqtt5::will{ status_topic, connection_status(false), boost::mqtt5::qos_e::exactly_once, boost::mqtt5::retain_e::yes });
            client.async_run([](boost::mqtt5::error_code) {});

            const boost::mqtt5::publish_props props{};
            client.async_publish<boost::mqtt5::qos_e::exactly_once>(status_topic, connection_status(true), boost::mqtt5::retain_e::yes, props,
                [](boost::mqtt5::error_code, auto, auto) {});
            client.async_publish<boost::mqtt5::qos_e::exactly_once>(broker_topic, utf8(state.serialize()), boost::mqtt5::retain_e::yes, props,
                [](boost::mqtt5::error_code, auto, auto) {});

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
                const boost::mqtt5::publish_props props{};
                client.async_publish<boost::mqtt5::qos_e::exactly_once>(status_topic, connection_status(false), boost::mqtt5::retain_e::yes, props,
                    [this](boost::mqtt5::error_code, auto, auto)
                    {
                        shutdown_timer.cancel();
                        client.async_disconnect([](boost::mqtt5::error_code) {});
                    });
                // Permit the retained graceful-shutdown status to complete before
                // starting disconnect. async_disconnect may still take up to about
                // five seconds when the broker is unavailable.
                shutdown_timer.expires_after(std::chrono::seconds(1));
                shutdown_timer.async_wait([this](boost::mqtt5::error_code error)
                {
                    if (!error) client.async_disconnect([](boost::mqtt5::error_code) {});
                });
            });
        }
    };

    events_mqtt_sender::events_mqtt_sender(const nmos::id& sender_id, const web::json::value& transport_params, const web::json::value& state)
        : impl_(new impl(sender_id, transport_params, state))
    {}

    events_mqtt_sender::~events_mqtt_sender() = default;

    void events_mqtt_sender::stop()
    {
        impl_->stop();
    }
}
