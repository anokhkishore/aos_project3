#pragma once
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <vector>
#include <future>

class threadpool {
public:
	threadpool(int num_threads, void (*thread_function)(void*));
    void enqueue_task(void *task_args);
    
    /*
    template<typename T>
    std::future<T> enqueue_task(void *task_args, std::promise<T>&& promise);
    */

    void wait_for_all_tasks();
    void kill_threads();

    ~threadpool();
private:
    int num_threads;
    std::vector<std::thread> threads;
    void thread_function_wrapper(void *);
    std::queue<void*> task_queue;
    void (*thread_function)(void*);

    // sync primitives
    std::mutex mtx_;
    std::condition_variable cv_;
    std::condition_variable cv_task_done;
    bool stop_workers;
};


