#include "threadpool.h"
#include <thread>
#include <iostream>
#include <memory>
#include <fstream>

#include <grpc/support/log.h>
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

using namespace store;
using namespace vendor;


class StoreImpl;

class StoreCallData {
public:
    StoreCallData(Store::AsyncService *pservice, ServerCompletionQueue* pcq,
	  std::vector<std::shared_ptr<Channel>>& vendor_channels, threadpool *tp_process)
	  :pservice_(pservice), pcq_(pcq), responder_(&sctx_), status_(CREATE), 
	  vendor_channels_(vendor_channels), tp_process_(tp_process)
	{
		Proceed();
	}

	void Proceed() {
		if (status_ == CREATE) {
			status_ = PROCESS;

			// Registers the callData instance for the next rpc call for getProducts
			// in ServerCompletionQueue
			pservice_->RequestgetProducts(&sctx_, &request, &responder_, pcq_, pcq_, this);
		}
		else if (status_ == PROCESS) {
			// this part reached only when a new rpc call is triggered via client.
			// Create a new StoreCallData instance to handle yet another new rpc
			new StoreCallData(pservice_, pcq_, vendor_channels_, tp_process_);

			// enqueue the processing of this request in threadpool.
			tp_process_->enqueue_task(this);
			return;
		}
		else {
			if(status_ == FINISH) {
			    delete this;
			}
		}
	}

// private:
	ServerContext sctx_;
	Store::AsyncService *pservice_;
	ServerCompletionQueue *pcq_;
	ServerAsyncResponseWriter<ProductReply> responder_;
	std::vector<std::shared_ptr<Channel>>& vendor_channels_;
	threadpool* tp_process_;

	ProductQuery request;
	ProductReply reply;

	enum CallStatus {CREATE, PROCESS, FINISH};
	CallStatus status_;
};

void storeCallDataHandler(void *arg) {
	StoreCallData *callData = (StoreCallData*) arg;
	auto& vendor_channels = callData->vendor_channels_;
	ProductReply& reply = callData->reply;
	ProductQuery& request = callData->request;
	auto& responder = callData->responder_;

	for (const auto& channel : vendor_channels) {
		ClientContext clientContext;
		BidQuery bidQuery;
		BidReply bidReply;

		bidQuery.set_product_name(request.product_name());
		
		std::unique_ptr<Vendor::Stub> stub = Vendor::NewStub(channel);
		// std::cout << "Sending vendor request for product = " << bidQuery.product_name() << std::endl;
		
		Status status = stub->getProductBid(&clientContext, bidQuery, &bidReply);
		if (!status.ok()) {
			std::cout << status.error_code() << ": " << status.error_message() << std::endl;		
		}

		auto* product = reply.mutable_products()->Add();
		product->set_price(bidReply.price());
		product->set_vendor_id(bidReply.vendor_id());
	}

	callData->status_ = callData->FINISH;
	responder.Finish(reply, Status::OK, callData);
}

class StoreImpl{
public:
	StoreImpl(std::vector<std::shared_ptr<Channel>>&& vendor_channels, int request_workers=4, int vendor_workers=8)
	  : vendor_channels(vendor_channels), tp_process(request_workers, storeCallDataHandler) {
	  }

	void RunServer(const std::string& server_address = "0.0.0.0:50058") {
		// std::string server_address("0.0.0.0:50053");
		ServerBuilder builder;
		builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
		builder.RegisterService(&service_);

		cq_ = builder.AddCompletionQueue();
		std::unique_ptr<Server> server(builder.BuildAndStart());
		std::cout << "store server listening on " << server_address << std::endl;

        HandleRpcs(); // instead of server->Wait();
	}	

private:
	std::vector<std::shared_ptr<Channel>>& vendor_channels;
	threadpool tp_process;
	Store::AsyncService service_;
	std::unique_ptr<ServerCompletionQueue> cq_;

	void HandleRpcs() {
		new StoreCallData(&service_, cq_.get(), vendor_channels, &tp_process);
		void *tag;
		bool ok;

		while (true) {
			if(cq_->Next(&tag, &ok) && ok) {
				static_cast<StoreCallData*>(tag)->Proceed();
			}
			else {
				fprintf(stderr, "Something went wrong with completion queue Next\n");
			}
		}
	}
};


int main(int argc, char** argv) {
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

