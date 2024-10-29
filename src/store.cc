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

using grpc::ServerCompletionQueue;
using grpc::ServerAsyncResponseWriter;
using grpc::ClientAsyncResponseReader;
using grpc::CompletionQueue;
using store::Store;
using store::ProductQuery;
using store::ProductReply;
using store::ProductInfo;

using namespace store;
using namespace vendor;


class StoreImpl;

enum CallStatus { CREATE, PROCESS, FINISH };

class VendorRequestWrapper{
public:
    ProductQuery query;
	ProductReply reply;
	// Status status;
	// std::shared_ptr<Channel> channel;
	std::vector<std::shared_ptr<Channel>> channels;
	// void *cd; // cast to a CallData later
	
	CallStatus *cdStatus;
	ServerAsyncResponseWriter<ProductReply> responder;
	void **tag;
};

std::mutex global_mtx;

// void vendor_request_handler(void *arg) {
// 	VendorRequestWrapper *preq = (VendorRequestWrapper*) arg;
// 	ClientContext context;

// 	std::unique_ptr<Vendor::Stub> stub = Vendor::NewStub(preq->channel);
// 	std::cout << "Sending vendor request for product = " << preq->query.product_name() << std::endl;
    
// 	preq->status = stub->getProductBid(&context, preq->query, &(preq->reply));
//  	if (!preq->status.ok()) {
// 		std::cout << preq->status.error_code() << ": " << preq->status.error_message() << std::endl;		
// 	}

// 	preq->done_promise.set_value(true);
// }

void vendor_request_handler(void *arg) { // will be made once per vendor
	VendorRequestWrapper *preq = (VendorRequestWrapper*) arg;
	ClientContext context;
	CompletionQueue cq;
	std::vector<std::unique_ptr<Vendor::Stub>> stubs;
	std::vector<BidQuery> bidQueries;
	std::vector<BidReply> bidReplies;
	std::vector<Status> statuses;
	std::vector<std::unique_ptr<ClientAsyncResponseReader<BidReply>>> rpcs;
	void *got_tag;
	bool ok;

	std::cout << "Sending vendor requests for product = " << preq->query.product_name() << std::endl;
	for (auto channel : preq->channels) {
		stubs.push_back(Vendor::NewStub(channel));
		BidQuery q;
		q.set_product_name(preq->query.product_name());
		bidQueries.push_back(q);
		bidReplies.emplace_back();

		rpcs.emplace_back(stubs.back()->AsyncgetProductBid(&context, bidQueries.back(), &cq));
		rpcs.back()->Finish(&bidReplies.back(), &statuses.back(), (void *)1); // write result directly to server's reply for client
		ProductInfo *newReply = preq->reply.mutable_products()->Add();
		newReply->set_price(bidReplies.back().price());
		newReply->set_vendor_id(bidReplies.back().vendor_id());
		cq.Next(&got_tag, &ok);
	}
	*(preq->cdStatus) = FINISH;
	preq->responder.Finish(preq->reply, Status::OK, preq->tag);
}


class StoreImpl : public Store::AsyncService{
public:
	StoreImpl(std::vector<std::shared_ptr<Channel>>&& vendor_channels, int request_workers=4)
	  : vendor_channels(vendor_channels), tp_vendor_request(request_workers, vendor_request_handler) {
		
	  }
	
	// TODO: deconstructor?

	// Status getProducts(ServerContext* context, const ProductQuery* request,
    //               ProductReply* reply) override {
	// 	// std::cout << "Starting getProducts for " << request->product_name() << std::endl;
	// 	std::vector<std::shared_ptr<VendorRequestWrapper>> reqs;
	// 	int i = 0;
	// 	std::vector<std::thread> vw;
	// 	for(const auto& vc : vendor_channels) {
	// 		if (i < 4) {
	// 		std::shared_ptr<VendorRequestWrapper> req = std::make_shared<VendorRequestWrapper>();
	// 		req->query.set_product_name(request->product_name());
	// 		req->channel = vc;
	// 		tp_vendor_request.enqueue_task((void*)req.get());
	// 		reqs.push_back(req);
	// 		}
	// 		++i;
	// 	}

	// 	for(auto& req: reqs) {
	// 	    std::future<bool> done = req->done_promise.get_future();
	// 		done.get();

	// 		if (!req->status.ok()) {
	// 			std::cout << "Error :" << req->status.error_message() <<"with request for " << req->query.product_name() << std::endl;
	// 			continue;
	// 		}

	// 		auto* product = reply->mutable_products()->Add();
	// 		product->set_price(req->reply.price());
	// 		product->set_vendor_id(req->reply.vendor_id());
	// 	}

	// 	return Status::OK;
	// }

	void Run(const std::string& server_address = "0.0.0.0:50058") {
		ServerBuilder builder;
		builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
		builder.RegisterService(this);


		cq_ = builder.AddCompletionQueue();
		server_ = builder.BuildAndStart();
		std::cout << "store server listening on " << server_address << std::endl;

		HandleRpcs();
	}

	class CallData {
		public:
		CallData(Store::AsyncService* service, ServerCompletionQueue* cq)
			: service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
		// Invoke the serving logic right away.
		Proceed();
		}

		void Proceed() {
			if (status_ == CREATE) {
				// Make this instance progress to the PROCESS state.
				status_ = PROCESS;

				// As part of the initial CREATE state, we *request* that the system
				// start processing SayHello requests. In this request, "this" acts are
				// the tag uniquely identifying the request (so that different CallData
				// instances can serve different requests concurrently), in this case
				// the memory address of this CallData instance.
				service_->RequestgetProducts(&ctx_, &request_, &responder_, cq_, cq_,
										this);
			} else if (status_ == PROCESS) {
				// Spawn a new CallData instance to serve new clients while we process
				// the one for this CallData. The instance will deallocate itself as
				// part of its FINISH state.
				new CallData(service_, cq_);

				/*
					here, enqueue task
				*/
				std::shared_ptr<VendorRequestWrapper> req = std::make_shared<VendorRequestWrapper>();
				req->query.set_product_name(request_.product_name());
				req->channels = vendor_channels;
				tp_vendor_request.enqueue_task((void*)req.get());
				req->cdStatus = &status_;
				req->responder = responder_;
				req->tag = (void **)this;

				
			} else {
				// Once in the FINISH state, deallocate ourselves (CallData).
				delete this;
			}
		}

		
		
		CallStatus status_;  // The current serving state.

		private:
		// The means of communication with the gRPC runtime for an asynchronous
		// server.
		Store::AsyncService* service_;
		// The producer-consumer queue where for asynchronous server notifications.
		ServerCompletionQueue* cq_;
		// Context for the rpc, allowing to tweak aspects of it such as the use
		// of compression, authentication, as well as to send metadata back to the
		// client.
		ServerContext ctx_;

		// What we get from the client.
		ProductQuery request_;
		// What we send back to the client.
		ProductReply reply_;

		// The means to get back to the client.
		ServerAsyncResponseWriter<ProductReply> responder_;

		// Let's implement a tiny state machine with the following states.
	};


private:
	

	void HandleRpcs() {
		new CallData(&service_, cq_.get());
		void *tag;
		bool ok;
		while(true) {
			cq_->Next(&tag, &ok);
			static_cast<CallData *>(tag)->Proceed();
		}
	}

	std::vector<std::shared_ptr<Channel>>& vendor_channels;
	threadpool tp_vendor_request;

	//alex
	std::shared_ptr<ServerCompletionQueue> cq_; // accessed by all worker threads to send replies back to clients
	Store::AsyncService service_;
	std::unique_ptr<Server> server_;
	
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

	// TODO: accept a port as a command line argument

    std::vector<std::string> vendor_server_addrs;
    std::ifstream myfile ("vendor_addresses.txt");
	if (myfile.is_open()) {
		std::string ip_addr;
		while (getline(myfile, ip_addr)) {
			vendor_server_addrs.push_back(ip_addr);
		}
		myfile.close();
	}
    
	// std::cout << "size of vendor_server_addrs = " << vendor_server_addrs.size() << std::endl;
	std::vector<std::shared_ptr<Channel>> vendor_channels;
	for(const auto& server_addr: vendor_server_addrs) {
		std::cout << "Creating channel for vendor ip address " << server_addr << std::endl;
        vendor_channels.push_back(grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials()));
	}

	StoreImpl st(std::move(vendor_channels));
	st.Run();

	return EXIT_SUCCESS;
}

