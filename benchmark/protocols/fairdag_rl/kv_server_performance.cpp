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

#include <glog/logging.h>

#include <cstdlib>
#include <string>

#include "chain/storage/memory_db.h"
#include "executor/kv/kv_executor.h"
#include "platform/config/resdb_config_utils.h"
#include "platform/consensus/ordering/fairdag_rl/framework/consensus.h"
#include "platform/networkstrate/service_network.h"
#include "platform/statistic/stats.h"
#include "proto/kv/kv.pb.h"

using namespace resdb;
using namespace resdb::fairdag_rl;
using namespace resdb::storage;

void ShowUsage() {
  printf("<config> <private_key> <cert_file> [logging_dir]\n");
}

std::string GetRandomKey() {
  int num1 = rand() % 10;
  int num2 = rand() % 10;
  return std::to_string(num1) + std::to_string(num2);
}

int main(int argc, char** argv) {
  if (argc < 3) {
    ShowUsage();
    exit(0);
  }

  // google::InitGoogleLogging(argv[0]);
  // FLAGS_minloglevel = google::GLOG_WARNING;

  char* config_file = argv[1];
  char* private_key_file = argv[2];
  char* cert_file = argv[3];

  if (argc >= 5) {
    auto monitor_port = Stats::GetGlobalStats(5);
    monitor_port->SetPrometheus(argv[4]);
  }

  std::unique_ptr<ResDBConfig> config =
      GenerateResDBConfig(config_file, private_key_file, cert_file);

  config->RunningPerformance(true);

  auto performance_consens = std::make_unique<FairDAGConsensus>(
      *config, std::make_unique<KVExecutor>(std::make_unique<MemoryDB>()));

  size_t value_size = std::string("helloworld").size();
  const char* tx_size_env = std::getenv("FAIRDAG_TX_SIZE");
  if (tx_size_env != nullptr) {
    const size_t target_size = std::strtoull(tx_size_env, nullptr, 10);
    KVRequest sizing_request;
    sizing_request.set_cmd(KVRequest::SET);
    sizing_request.set_key("00");
    std::string value(target_size, 'x');
    sizing_request.set_value(value);
    for (int i = 0; i < 4 && sizing_request.ByteSizeLong() != target_size; ++i) {
      const size_t serialized_size = sizing_request.ByteSizeLong();
      if (serialized_size > target_size) {
        value.resize(value.size() - (serialized_size - target_size));
      } else {
        value.append(target_size - serialized_size, 'x');
      }
      sizing_request.set_value(value);
    }
    CHECK_EQ(sizing_request.ByteSizeLong(), target_size);
    value_size = value.size();
    LOG(WARNING) << "[BENCH] serialized_transaction_size:" << target_size;
  }

  performance_consens->SetupPerformanceDataFunc([value_size]() {
    KVRequest request;
    request.set_cmd(KVRequest::SET);
    request.set_key(GetRandomKey());
    request.set_value(std::string(value_size, 'x'));
    std::string request_data;
    request.SerializeToString(&request_data);
    return request_data;
  });

  auto server =
      std::make_unique<ServiceNetwork>(*config, std::move(performance_consens));
  server->Run();
}
