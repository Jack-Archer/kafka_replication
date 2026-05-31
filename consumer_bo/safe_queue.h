#pragma once



#include <queue>
#include <mutex>
#include <condition_variable>

template <typename T>
class SafeQueue
{
    public:
        explicit SafeQueue(size_t max_size) : max_queue_size_(max_size) {}

        void push(T consumer_object)
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            cond_push_.wait(lock,[this](){return queue_to_save_.size() < max_queue_size_;});
            queue_to_save_.push(std::move(consumer_object));

            lock.unlock();
            cond_pop_.notify_one();
        }

        T pop()
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            cond_pop_.wait(lock, [this](){return !queue_to_save_.empty();});

            T value = std::move(queue_to_save_.front());
            queue_to_save_.pop();

            lock.unlock();
            cond_push_.notify_one();
            return value;
        }

    private:
        std::queue<T> queue_to_save_;
        size_t max_queue_size_;
        std::condition_variable cond_push_;
        std::condition_variable cond_pop_;
        std::mutex queue_mutex_;
};