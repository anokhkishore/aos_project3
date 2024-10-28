#include "threadpool.h"
#include <thread>
#include <iostream>
#include <memory>
#include <fstream>
#include <grpcpp/grpcpp.h>
#include "store.grpc.pb.h"
#include "vendor.grpc.pb.h"


using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Server;

using namespace store;
using namespace vendor;


class StoreImpl;

class VendorRequestWrapper{
public:
    BidQuery query;
	BidReply reply;
	Status status;
	const std::unique_ptr<Vendor::Stub> *pstub;  // pointer to a std::unique_ptr !
	std::promise<bool> done_promise; 
};

void vendor_request_handler(void *arg) {
	VendorRequestWrapper *preq = (VendorRequestWrapper*) arg;

	ClientContext context;
	const std::unique_ptr<Vendor::Stub> *pstub = preq->pstub;
	std::cout << "Sending vendor request for product = " << preq->query.product_name() << std::endl;

	preq->status = (*pstub)->getProductBid(&context, preq->query, &(preq->reply));
 	if (!preq->status.ok()) {
		std::cout << preq->status.error_code() << ": " << preq->status.error_message() << std::endl;		
	}

	preq->done_promise.set_value(true);
}

class StoreImpl : public Store::Service{
public:
	StoreImpl(const std::vector<std::shared_ptr<Channel>>& vendor_channels, int request_workers=4, int vendor_workers=8)
	  : tp_vendor_request(request_workers, vendor_request_handler) {
		for(const auto& channel: vendor_channels) {
		    stubs_.emplace_back(Vendor::NewStub(channel));
		}
	  }

	Status getProducts(ServerContext* context, const ProductQuery* request,
                  ProductReply* reply) override {
		// std::cout << "Starting getProducts for " << request->product_name() << std::endl;  
		std::vector<VendorRequestWrapper> reqs;
		int i = 0;
		for(const auto& stub : stubs_) {
			if (3 == i || 2 == i) {
		    reqs.emplace_back();
			auto& req = reqs.back();
			req.query.set_product_name(request->product_name());
			req.pstub = &stub;
			// std::cout << "Enqueueing vendor request for " << request->product_name() << std::endl;  
			std::cout << "req ptr = " << &req << " pstub = " << &stub << std::endl;
			tp_vendor_request.enqueue_task(&req);
			}
			++i;
		}		

		for(auto& req: reqs) {
		    std::future<bool> done = req.done_promise.get_future();
			done.get();
			if (!req.status.ok()) {
				std::cout << "Error :" << req.status.error_message() <<"with request for " << req.query.product_name() << std::endl;
				continue;
			}

			auto* product = reply->mutable_products()->Add();
			product->set_price((req.reply).price());
			product->set_vendor_id((req.reply).vendor_id());
		}

		return Status::OK;
	}

	void RunServer(const std::string& server_address = "0.0.0.0:50058") {
		// std::string server_address("0.0.0.0:50053");
		ServerBuilder builder;
		builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
		builder.RegisterService(this);

		std::unique_ptr<Server> server(builder.BuildAndStart());
		std::cout << "store server listening on " << server_address << std::endl;

		server->Wait();
	}	

private:
    std::vector<std::unique_ptr<Vendor::Stub>> stubs_;
	threadpool tp_vendor_request;
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
	/*
	std::cout << "I 'm not ready yet!" << std::endl;
	threadpool pool(5, tfn);

    int t = 1, q = 2;
	pool.enqueue_task(&t);
	pool.enqueue_task(&q);
	pool.wait_for_all_tasks();
	*/

    std::vector<std::string> vendor_server_addrs;
    std::ifstream myfile ("vendor_addresses.txt");
	if (myfile.is_open()) {
		std::string ip_addr;
		while (getline(myfile, ip_addr)) {
			vendor_server_addrs.push_back(ip_addr);
		}
		myfile.close();
	}
    
	std::cout << "size of vendor_server_addrs = " << vendor_server_addrs.size() << std::endl;
	std::vector<std::shared_ptr<Channel>> vendor_channels;
	for(const auto& server_addr: vendor_server_addrs) {
		std::cout << "Creating channel for vendor ip address " << server_addr << std::endl;
        vendor_channels.push_back(grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials()));
	}

	StoreImpl st(vendor_channels);
	st.RunServer();

	return EXIT_SUCCESS;
}

