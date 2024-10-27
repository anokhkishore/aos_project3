// Generated by the gRPC C++ plugin.
// If you make any local change, they will be lost.
// source: protos/vendor.proto

#include "protos/vendor.pb.h"
#include "protos/vendor.grpc.pb.h"

#include <functional>
#include <grpcpp/impl/codegen/async_stream.h>
#include <grpcpp/impl/codegen/async_unary_call.h>
#include <grpcpp/impl/codegen/channel_interface.h>
#include <grpcpp/impl/codegen/client_unary_call.h>
#include <grpcpp/impl/codegen/client_callback.h>
#include <grpcpp/impl/codegen/message_allocator.h>
#include <grpcpp/impl/codegen/method_handler.h>
#include <grpcpp/impl/codegen/rpc_service_method.h>
#include <grpcpp/impl/codegen/server_callback.h>
#include <grpcpp/impl/codegen/server_callback_handlers.h>
#include <grpcpp/impl/codegen/server_context.h>
#include <grpcpp/impl/codegen/service_type.h>
#include <grpcpp/impl/codegen/sync_stream.h>
namespace vendor {

static const char* Vendor_method_names[] = {
  "/vendor.Vendor/getProductBid",
};

std::unique_ptr< Vendor::Stub> Vendor::NewStub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options) {
  (void)options;
  std::unique_ptr< Vendor::Stub> stub(new Vendor::Stub(channel));
  return stub;
}

Vendor::Stub::Stub(const std::shared_ptr< ::grpc::ChannelInterface>& channel)
  : channel_(channel), rpcmethod_getProductBid_(Vendor_method_names[0], ::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  {}

::grpc::Status Vendor::Stub::getProductBid(::grpc::ClientContext* context, const ::vendor::BidQuery& request, ::vendor::BidReply* response) {
  return ::grpc::internal::BlockingUnaryCall< ::vendor::BidQuery, ::vendor::BidReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_getProductBid_, context, request, response);
}

void Vendor::Stub::experimental_async::getProductBid(::grpc::ClientContext* context, const ::vendor::BidQuery* request, ::vendor::BidReply* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::vendor::BidQuery, ::vendor::BidReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_getProductBid_, context, request, response, std::move(f));
}

void Vendor::Stub::experimental_async::getProductBid(::grpc::ClientContext* context, const ::vendor::BidQuery* request, ::vendor::BidReply* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_getProductBid_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::vendor::BidReply>* Vendor::Stub::PrepareAsyncgetProductBidRaw(::grpc::ClientContext* context, const ::vendor::BidQuery& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::vendor::BidReply, ::vendor::BidQuery, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_getProductBid_, context, request);
}

::grpc::ClientAsyncResponseReader< ::vendor::BidReply>* Vendor::Stub::AsyncgetProductBidRaw(::grpc::ClientContext* context, const ::vendor::BidQuery& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncgetProductBidRaw(context, request, cq);
  result->StartCall();
  return result;
}

Vendor::Service::Service() {
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      Vendor_method_names[0],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< Vendor::Service, ::vendor::BidQuery, ::vendor::BidReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](Vendor::Service* service,
             ::grpc::ServerContext* ctx,
             const ::vendor::BidQuery* req,
             ::vendor::BidReply* resp) {
               return service->getProductBid(ctx, req, resp);
             }, this)));
}

Vendor::Service::~Service() {
}

::grpc::Status Vendor::Service::getProductBid(::grpc::ServerContext* context, const ::vendor::BidQuery* request, ::vendor::BidReply* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}


}  // namespace vendor

