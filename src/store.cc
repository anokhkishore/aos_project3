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
	std::shared_ptr<Channel> channel;
	std::promise<bool> done_promise; 
};

std::mutex global_mtx;

void vendor_request_handler(void *arg) {
	VendorRequestWrapper *preq = (VendorRequestWrapper*) arg;
	ClientContext context;

	std::unique_ptr<Vendor::Stub> stub = Vendor::NewStub(preq->channel);
	std::cout << "Sending vendor request for product = " << preq->query.product_name() << std::endl;
    
	preq->status = stub->getProductBid(&context, preq->query, &(preq->reply));
 	if (!preq->status.ok()) {
		std::cout << preq->status.error_code() << ": " << preq->status.error_message() << std::endl;		
	}

	preq->done_promise.set_value(true);
}

class StoreImpl : public Store::Service{
public:
	StoreImpl(std::vector<std::shared_ptr<Channel>>&& vendor_channels, int request_workers=4, int vendor_workers=8)
	  : vendor_channels(vendor_channels), tp_vendor_request(request_workers, vendor_request_handler) {
	  }

	Status getProducts(ServerContext* context, const ProductQuery* request,
                  ProductReply* reply) override {
		// std::cout << "Starting getProducts for " << request->product_name() << std::endl;
		std::vector<std::shared_ptr<VendorRequestWrapper>> reqs;
		int i = 0;
		std::vector<std::thread> vw;
		for(const auto& vc : vendor_channels) {
			if (i < 4) {
			std::shared_ptr<VendorRequestWrapper> req = std::make_shared<VendorRequestWrapper>();
			req->query.set_product_name(request->product_name());
			req->channel = vc;
			tp_vendor_request.enqueue_task((void*)req.get());
			reqs.push_back(req);
			}
			++i;
		}

		for(auto& req: reqs) {
		    std::future<bool> done = req->done_promise.get_future();
			done.get();

			if (!req->status.ok()) {
				std::cout << "Error :" << req->status.error_message() <<"with request for " << req->query.product_name() << std::endl;
				continue;
			}

			auto* product = reply->mutable_products()->Add();
			product->set_price(req->reply.price());
			product->set_vendor_id(req->reply.vendor_id());
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
	std::vector<std::shared_ptr<Channel>>& vendor_channels;
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
	/*  Test for threadpool working .. can add more stuff to check
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

	StoreImpl st(std::move(vendor_channels));
	st.RunServer();

	return EXIT_SUCCESS;
}

