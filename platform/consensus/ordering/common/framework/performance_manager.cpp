/*
 * Copyright (c) 2019-2022 ExpoLab, UC Davis
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 */

#include "platform/consensus/ordering/common/framework/performance_manager.h"

#include <chrono>
#include <cstdlib>

#include <glog/logging.h>

#include "common/utils/utils.h"

namespace resdb {
namespace common {

using comm::CollectorResultCode;

PerformanceManager::PerformanceManager(
    const ResDBConfig& config, ReplicaCommunicator* replica_communicator,
    SignatureVerifier* verifier)
     : config_(config),
      replica_communicator_(replica_communicator),
      batch_queue_("user request"),
      verifier_(verifier) {
  stop_ = false;
  eval_started_ = false;
  eval_ready_future_ = eval_ready_promise_.get_future();
  if (config_.GetPublicKeyCertificateInfo()
          .public_key()
          .public_key_info()
          .type() == CertificateKeyInfo::CLIENT) {
    for (int i = 0; i < 1; ++i) {
      user_req_thread_[i] =
          std::thread(&PerformanceManager::BatchProposeMsg, this);
    }
  }
  global_stats_ = Stats::GetGlobalStats();
  send_num_ = 0;
  total_num_ = 0;
  replica_num_ = config_.GetReplicaNum();
  id_ = config_.GetSelfInfo().id();
  primary_ = id_ % replica_num_;
  if (primary_ == 0) primary_ = replica_num_;
  local_id_ = 1;
  sum_ = 0;
  const char* rate = std::getenv("FAIRDAG_CLIENT_RATE");
  controlled_mode_ = rate != nullptr && std::strtoull(rate, nullptr, 10) > 0;
  target_rate_ = controlled_mode_ ? std::strtoull(rate, nullptr, 10) : 0;

  const char* duration = std::getenv("FAIRDAG_SEND_DURATION");
  send_duration_sec_ = duration == nullptr ? 60 : std::strtoull(duration, nullptr, 10);

  const char* warmup = std::getenv("FAIRDAG_WARMUP_DURATION");
  warmup_duration_sec_ =
      warmup == nullptr ? 0 : std::strtoull(warmup, nullptr, 10);

  const char* burst_hz = std::getenv("FAIRDAG_BURST_HZ");
  burst_hz_ = burst_hz == nullptr ? 20 : std::strtoull(burst_hz, nullptr, 10);

  const char* batch_delay = std::getenv("FAIRDAG_MAX_BATCH_DELAY_MS");
  max_batch_delay_ms_ = batch_delay == nullptr
                            ? config_.ClientBatchWaitTimeMS()
                            : std::strtoull(batch_delay, nullptr, 10);
  generated_transactions_ = 0;
  offered_transactions_ = 0;
  generation_done_ = false;
  summary_logged_ = false;
}

PerformanceManager::~PerformanceManager() {
  stop_ = true;
  if (generator_thread_.joinable()) {
    generator_thread_.join();
  }
  for (int i = 0; i < 16; ++i) {
    if (user_req_thread_[i].joinable()) {
      user_req_thread_[i].join();
    }
  }
}

int PerformanceManager::GetPrimary() { return primary_; }

std::unique_ptr<Request> PerformanceManager::GenerateUserRequest() {
  std::unique_ptr<Request> request = std::make_unique<Request>();
  request->set_data(data_func_());
  return request;
}

void PerformanceManager::SetDataFunc(std::function<std::string()> func) {
  data_func_ = std::move(func);
}

int PerformanceManager::StartEval() {
  if (eval_started_) {
    return 0;
  }
  eval_started_ = true;
  if (controlled_mode_) {
    generator_thread_ =
        std::thread(&PerformanceManager::GenerateRequestsAtRate, this);
    return 0;
  }
  for (int i = 0; i < 5000000; ++i) {
    // for (int i = 0; i < 60000000000; ++i) {
    std::unique_ptr<QueueItem> queue_item = std::make_unique<QueueItem>();
    queue_item->context = nullptr;
    queue_item->create_time = GetCurrentTime();
    queue_item->measure_latency = true;
    queue_item->user_request = GenerateUserRequest();
    batch_queue_.Push(std::move(queue_item));
    if (i == 200000) {
      eval_ready_promise_.set_value(true);
    }
  }
  LOG(WARNING) << "start eval done";
  return 0;
}

void PerformanceManager::GenerateRequestsAtRate() {
  LOG(WARNING) << "[BENCH] configured_rate:" << target_rate_
               << " send_duration_sec:" << send_duration_sec_
               << " burst_hz:" << burst_hz_
               << " max_batch_delay_ms:" << max_batch_delay_ms_;
  eval_ready_promise_.set_value(true);

  const auto start = std::chrono::steady_clock::now();
  const uint64_t total_ticks = send_duration_sec_ * burst_hz_;
  uint64_t generated = 0;
  for (uint64_t tick = 1; tick <= total_ticks && !stop_; ++tick) {
    std::this_thread::sleep_until(
        start + std::chrono::microseconds(tick * 1000000 / burst_hz_));
    const uint64_t target = target_rate_ * tick / burst_hz_;
    for (; generated < target; ++generated) {
      std::unique_ptr<QueueItem> queue_item = std::make_unique<QueueItem>();
      queue_item->context = nullptr;
      queue_item->create_time = GetCurrentTime();
      queue_item->measure_latency =
          tick > warmup_duration_sec_ * burst_hz_;
      queue_item->user_request = GenerateUserRequest();
      batch_queue_.Push(std::move(queue_item));
    }
    generated_transactions_ = generated;
  }
  generation_done_ = true;
}

// =================== response ========================
// handle the response message. If receive f+1 commit messages, send back to the
// user.
int PerformanceManager::ProcessResponseMsg(std::unique_ptr<Context> context,
                                           std::unique_ptr<Request> request) {
  std::unique_ptr<Request> response;
  // Add the response message, and use the call back to collect the received
  // messages.
  // The callback will be triggered if it received f+1 messages.
  if (request->ret() == -2) {
    // LOG(INFO) << "get response fail:" << request->ret();
    send_num_--;
    return 0;
  }

  //LOG(INFO) << "get response:" << request->seq() << " sender:"<<request->sender_id();
  std::unique_ptr<BatchUserResponse> batch_response = nullptr;
  CollectorResultCode ret =
      AddResponseMsg(std::move(request), [&](std::unique_ptr<BatchUserResponse> request) {
        batch_response = std::move(request);
        return;
      });

  if (ret == CollectorResultCode::STATE_CHANGED) {
    LOG(ERROR) << "[DK] RSP";
    assert(batch_response);
      SendResponseToClient(*batch_response);
  }
  return ret == CollectorResultCode::INVALID ? -2 : 0;
}

CollectorResultCode PerformanceManager::AddResponseMsg(
    std::unique_ptr<Request> request,
    std::function<void(std::unique_ptr<BatchUserResponse>)> response_call_back) {
  if (request == nullptr) {
    return CollectorResultCode::INVALID;
  }

  //uint64_t seq = request->seq();

  std::unique_ptr<BatchUserResponse> batch_response = std::make_unique<BatchUserResponse>();
  if (!batch_response->ParseFromString(request->data())) {
    LOG(ERROR) << "parse response fail:"<<request->data().size()
    <<" seq:"<<request->seq(); return CollectorResultCode::INVALID;
  }

  uint64_t seq = batch_response->local_id();
  BatchUserResponse response_values;
  for (const auto& response : batch_response->response()) {
    response_values.add_response(response);
  }
  std::string matching_response;
  response_values.SerializeToString(&matching_response);
  // LOG(ERROR)<<"receive seq:"<<seq;

  bool done = false;
  {
    int idx = seq % response_set_size_;
    std::unique_lock<std::mutex> lk(response_lock_[idx]);
    auto pending_response = response_[idx].find(seq);
    if (pending_response == response_[idx].end()) {
      //LOG(ERROR)<<"has done local seq:"<<seq<<" global seq:"<<request->seq();
      return CollectorResultCode::OK;
    }
    auto& matching_senders = pending_response->second[matching_response];
    matching_senders.insert(request->sender_id());
    // LOG(ERROR)<<"get seq :"<<request->seq()<<" local id:"<<seq<<" num:"<<matching_senders.size()<<" send:"<<send_num_;
    if (matching_senders.size() >=
        static_cast<size_t>(config_.GetMinClientReceiveNum())) {
      //LOG(ERROR)<<"get seq :"<<request->seq()<<" local id:"<<seq<<" num:"<<matching_senders.size()<<" done:"<<send_num_;
      response_[idx].erase(pending_response);
      done = true;
    }
  }
  if (done) {
    response_call_back(std::move(batch_response));
    return CollectorResultCode::STATE_CHANGED;
  }
  return CollectorResultCode::OK;
}

void PerformanceManager::SendResponseToClient(
    const BatchUserResponse& batch_response) {
  const uint64_t response_time = GetCurrentTime();
  if (controlled_mode_) {
    std::lock_guard<std::mutex> lock(batch_timing_mutex_);
    auto it = batch_timing_by_batch_.find(batch_response.local_id());
    if (it != batch_timing_by_batch_.end()) {
      const uint64_t batch_start = std::get<0>(it->second);
      const uint64_t pre_batch_latency = std::get<1>(it->second);
      const uint64_t transaction_count = std::get<2>(it->second);
      const uint64_t total_latency =
          pre_batch_latency +
          (response_time - batch_start) * transaction_count;
      if (transaction_count > 0) {
        global_stats_->AddLatency(total_latency, transaction_count);
      }
      batch_timing_by_batch_.erase(it);
      send_num_--;
      return;
    }
  }
  uint64_t create_time = batch_response.createtime();
  if (create_time > 0) {
    uint64_t run_time = response_time - create_time;
    //LOG(ERROR)<<"receive current:"<<GetCurrentTime()<<" create time:"<<create_time<<" run time:"<<run_time<<" local id:"<<batch_response.local_id();
    global_stats_->AddLatency(run_time);
  } else {
  }
  //send_num_-=10;
  send_num_--;
}

// =================== request ========================
int PerformanceManager::BatchProposeMsg() {
  LOG(WARNING) << "batch wait time:" << config_.ClientBatchWaitTimeMS()
               << " batch num:" << config_.ClientBatchNum()
               << " max txn:" << config_.GetMaxProcessTxn();
  std::vector<std::unique_ptr<QueueItem>> batch_req;
  eval_ready_future_.get();
  bool start = false;
  auto batch_start = std::chrono::steady_clock::now();
  while (!stop_) {
    if (!controlled_mode_ && send_num_ > config_.GetMaxProcessTxn()) {
      // LOG(ERROR)<<"wait send num:"<<send_num_;
      usleep(1000);
      continue;
    }
    if (batch_req.size() < config_.ClientBatchNum()) {
      int wait_ms = config_.ClientBatchWaitTimeMS();
      if (controlled_mode_ && !batch_req.empty()) {
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - batch_start);
        if (elapsed.count() >= static_cast<int64_t>(max_batch_delay_ms_)) {
          DoBatch(batch_req);
          batch_req.clear();
          continue;
        }
        wait_ms = static_cast<int>(max_batch_delay_ms_ - elapsed.count());
      }
      std::unique_ptr<QueueItem> item =
          batch_queue_.Pop(wait_ms);
      if (item == nullptr) {
        if (controlled_mode_ && !batch_req.empty()) {
          DoBatch(batch_req);
          batch_req.clear();
        }
        if(start){
          LOG(ERROR)<<"no data";
        }
        if (controlled_mode_ && generation_done_ && batch_queue_.Empty() &&
            !summary_logged_.exchange(true)) {
          LOG(WARNING) << "[BENCH] generated_transactions:"
                       << generated_transactions_
                       << " offered_transactions:" << offered_transactions_;
        }
        continue;
      }
      if (batch_req.empty()) {
        batch_start = std::chrono::steady_clock::now();
      }
      batch_req.push_back(std::move(item));
      if (batch_req.size() < config_.ClientBatchNum()) {
        continue;
      }
    }
    start = true;
    for(int i = 0; i < 1;++i){
      int ret = DoBatch(batch_req);
    }
    batch_req.clear();
  }
  return 0;
}

int PerformanceManager::DoBatch(
    const std::vector<std::unique_ptr<QueueItem>>& batch_req) {
  const uint64_t batch_start = GetCurrentTime();
  uint64_t pre_batch_latency = 0;
  uint64_t measured_transactions = 0;
  for (const auto& item : batch_req) {
    if (item->measure_latency) {
      pre_batch_latency += batch_start - item->create_time;
      measured_transactions++;
    }
  }

  auto new_request = comm::NewRequest(Request::TYPE_NEW_TXNS, Request(),
                                      config_.GetSelfInfo().id());
  if (new_request == nullptr) {
    return -2;
  }

  BatchUserRequest batch_request;
  for (size_t i = 0; i < batch_req.size(); ++i) {
    BatchUserRequest::UserRequest* req = batch_request.add_user_requests();
    *req->mutable_request() = *batch_req[i]->user_request.get();
    req->set_id(i);
  }

  batch_request.set_local_id(local_id_++);

  {
    int idx = batch_request.local_id() % response_set_size_;
    std::unique_lock<std::mutex> lk(response_lock_[idx]);
    response_[idx].emplace(
        batch_request.local_id(),
        std::map<std::string, std::set<int32_t>>());
  }

  batch_request.set_proxy_id(config_.GetSelfInfo().id());
  batch_request.set_createtime(GetCurrentTime());
  batch_request.SerializeToString(new_request->mutable_data());
  if (verifier_) {
    auto signature_or = verifier_->SignMessage(new_request->data());
    if (!signature_or.ok()) {
      LOG(ERROR) << "Sign message fail";
      return -2;
    }
    *new_request->mutable_data_signature() = *signature_or;
  }

  new_request->set_hash(SignatureVerifier::CalculateHash(new_request->data()));
  new_request->set_proxy_id(config_.GetSelfInfo().id());
  new_request->set_user_seq(batch_request.local_id());

  if (controlled_mode_) {
    std::lock_guard<std::mutex> lock(batch_timing_mutex_);
    batch_timing_by_batch_[batch_request.local_id()] = std::make_tuple(
        batch_start, pre_batch_latency, measured_transactions);
  }
  SendMessage(*new_request);

  global_stats_->BroadCastMsg();
  send_num_++;
  sum_ += batch_req.size();
  if (controlled_mode_) {
    offered_transactions_ += batch_req.size();
  }
  //LOG(ERROR)<<"send num:"<<send_num_<<" total num:"<<total_num_<<" sum:"<<sum_<<" to:"<<GetPrimary();
  if (total_num_++ == 1000000 && !controlled_mode_) {
    stop_ = true;
    LOG(WARNING) << "total num is done:" << total_num_;
  }
  if (total_num_ % 1000 == 0) {
    LOG(WARNING) << "total num is :" << total_num_;
  }
  global_stats_->IncClientCall();
  return 0;
}

void PerformanceManager::SendMessage(const Request& request){
  replica_communicator_->SendMessage(request, GetPrimary());
}

}  // namespace common
}  // namespace resdb
