#include <rxcpp/rx.hpp>
#include <unordered_map>
#include <atomic>
#include <memory>
#include <chrono>
#include <mutex>
#include <rxcpp/schedulers/rx-newthread.hpp>
#include <iostream>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

namespace rx = rxcpp;
using namespace std::chrono_literals;

template<typename K, typename V>
auto conflate_by_key(std::chrono::milliseconds interval) {
    using KeyValuePair = std::pair<K, V>;
    using BufferMap = std::unordered_map<K, V>;

    return [interval](rx::observable<KeyValuePair> source) {
        return rx::observable<>::create<KeyValuePair>(
            [source, interval](rx::subscriber<KeyValuePair> subscriber) {
                // Shared state for the operator
                auto buffer1 = std::make_shared<BufferMap>();
                auto buffer2 = std::make_shared<BufferMap>();
                auto current_buffer = std::make_shared<std::atomic<BufferMap*>>(buffer1.get());
                auto buffer_mutex = std::make_shared<std::mutex>();
                auto is_completed = std::make_shared<std::atomic<bool>>(false);

                // Subscribe to source
                auto source_sub = source.subscribe(
                    [current_buffer, buffer_mutex](const KeyValuePair& pair) {
                        std::lock_guard<std::mutex> lock(*buffer_mutex);
                        BufferMap* current = current_buffer->load();
                        (*current)[pair.first] = pair.second;
                    },
                    [subscriber](std::exception_ptr ep) {
                        subscriber.on_error(ep);
                    },
                    [is_completed, subscriber]() {
                        is_completed->store(true);
                        subscriber.on_completed();
                    }
                );

                // Timer subscription
                auto timer_sub = rx::observable<>::interval(interval)
                    .subscribe(
                        [buffer1, buffer2, current_buffer, buffer_mutex, subscriber, is_completed](long) {
                            if (is_completed->load()) return;

                            std::lock_guard<std::mutex> lock(*buffer_mutex);

                            // Switch buffers
                            BufferMap* old_buffer = current_buffer->load();
                            BufferMap* new_buffer = (old_buffer == buffer1.get())
                                ? buffer2.get()
                                : buffer1.get();
                            current_buffer->store(new_buffer);

                            // Emit all entries from old buffer
                            for (const auto& entry : *old_buffer) {
                                subscriber.on_next(entry);
                            }

                            // Clear the old buffer
                            old_buffer->clear();
                        },
                        [subscriber](std::exception_ptr ep) {
                            subscriber.on_error(ep);
                        }
                    );

                // Add cleanup
                subscriber.add([source_sub, timer_sub]() {
                    source_sub.unsubscribe();
                    timer_sub.unsubscribe();
                });
            }
        );
    };
}



// Extension method approach (most similar to original Java)
namespace rxcpp_extensions {
    template<typename K, typename V>
    struct conflate_by_key_t {
        std::chrono::milliseconds interval;

        auto operator()(rx::observable<std::pair<K, V>> source) const {
            return conflate_by_key<K, V>(interval)(source);
        }
    };

    template<typename K, typename V>
    auto conflate_by_key(std::chrono::milliseconds interval) {
        return conflate_by_key_t<K, V>{interval};
    }
}

int main() {
    //example_usage();
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [thread %t] [%l] %v");


    // use subject

    auto new_thread_scheduler = rxcpp::synchronize_new_thread();

    rx::subjects::subject<std::pair<std::string, int>> subject;


    auto conflated = subject.get_observable()
  | rxcpp_extensions::conflate_by_key<std::string, int>(std::chrono::milliseconds(3000));


    conflated.subscribe_on(new_thread_scheduler).subscribe([](const std::pair<std::string, int>& pair) {
        //std::cout << "Emitted: " << pair.first << " -> " << pair.second << std::endl;
        spdlog::info("Emitted: {} -> {}", pair.first, pair.second);
    }
);


    rxcpp::observable<>::interval(500ms).subscribe_on(new_thread_scheduler)
        .subscribe([&](int i) {
            auto item  = std::make_pair(std::string("Key1"), i);
            spdlog::info("Publishing #1 item: {} -> {}", item.first, item.second);
            subject.get_subscriber().on_next(item);
        });


    auto source = rxcpp::observable<>::interval(500ms).subscribe_on(new_thread_scheduler)
    .subscribe([&](int i) {
        auto item  = std::make_pair(std::string("Key2"), i);
        spdlog::info("Publishing #2 item: {} -> {}", item.first, item.second);

        subject.get_subscriber().on_next(item);

    });






    //
    // auto conflated = source
    //     | rxcpp_extensions::conflate_by_key<std::string, int>(std::chrono::milliseconds(2000));




    // wait for keyboard input
    spdlog::info("Press Enter to exit...");
    std::cout << "Press Enter to exit..." << std::endl;
    std::cin.get();
    return 0;
}