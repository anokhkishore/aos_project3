#include "threadpool.h"
#include <iostream>

threadpool::threadpool(int num_threads, void (*thread_function)(void*)) 
: num_threads(num_threads), thread_function(thread_function), stop_workers(false) {
    for (int i = 0 ; i < num_threads; ++i) {
        threads.emplace_back(&threadpool::thread_function_wrapper, this, nullptr);
    }
}

void threadpool::thread_function_wrapper(void * arg) {
    while (!stop_workers) {
        std::unique_lock<std::mutex> lock(mtx_);
        cv_.wait(lock, [this]{ return !task_queue.empty() || stop_workers; });
        if (task_queue.empty()) {
            continue;
        }
        void *args = task_queue.front();
        task_queue.pop();
        lock.unlock();
        this->thread_function(args);
        cv_task_done.notify_one();
    }
}

void threadpool::enqueue_task(void *task_args) {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        task_queue.push(task_args);
    }

    cv_.notify_one();
}

// Use carefully. Dont enqueue after calling this.
void threadpool::wait_for_all_tasks() {
    std::unique_lock<std::mutex> lock(mtx_);
    cv_task_done.wait(lock, [this]{ return task_queue.empty();});
}

void threadpool::kill_threads(){
    {
        std::lock_guard<std::mutex> lock(mtx_);
        stop_workers = true;
    }
    
    cv_.notify_all();

    for(int i = 0; i < num_threads; ++i) {
        if (threads[i].joinable()) {
            threads[i].join();
        }
    }
}

threadpool::~threadpool() {
    printf("threadpool Destructor\n");
    kill_threads();
    
}