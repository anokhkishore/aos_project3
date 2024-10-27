#include "threadpool.h"
#include <thread>
#include <iostream>

class store { 

};

static std::mutex console_mtx_;

void tfn(void *arg) {
	int x = *(int*)arg;
	{
		std::lock_guard<std::mutex> lock(console_mtx_);
		std::cout << "Hello from thread id: " << std::this_thread::get_id() 
		<< " Got arg = " << x << std::endl;
	}
}

int main(int argc, char** argv) {
	std::cout << "I 'm not ready yet!" << std::endl;
	threadpool pool(5, tfn);

    int t = 1, q = 2;
	pool.enqueue_task(&t);
	pool.enqueue_task(&q);
	pool.wait_for_all_tasks();
	return EXIT_SUCCESS;
}

