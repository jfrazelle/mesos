// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef __PIDS_ISOLATOR_HPP__
#define __PIDS_ISOLATOR_HPP__

#include <sys/types.h>

#include <string>
#include <vector>

#include <process/future.hpp>

#include <stout/hashmap.hpp>
#include <stout/option.hpp>

#include "slave/flags.hpp"

#include "slave/containerizer/mesos/isolator.hpp"

#include "slave/containerizer/mesos/isolators/cgroups/constants.hpp"

namespace mesos {
namespace internal {
namespace slave {

// Use the Linux pids cgroup controller for isolating the number of pids
// spawned.
class CgroupsPidsIsolatorProcess : public MesosIsolatorProcess
{
public:
  static Try<mesos::slave::Isolator*> create(const Flags& flags);

  virtual ~CgroupsPidsIsolatorProcess();

  virtual process::Future<Nothing> recover(
      const std::list<mesos::slave::ContainerState>& states,
      const hashset<ContainerID>& orphans);

  virtual process::Future<Option<mesos::slave::ContainerLaunchInfo>> prepare(
      const ContainerID& containerId,
      const mesos::slave::ContainerConfig& containerConfig);

  virtual process::Future<Nothing> isolate(
      const ContainerID& containerId,
      pid_t pid);

  virtual process::Future<mesos::slave::ContainerLimitation> watch(
      const ContainerID& containerId);

  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources);

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

  virtual process::Future<Nothing> cleanup(
      const ContainerID& containerId);

private:
  CgroupsPidsIsolatorProcess(
      const Flags& flags,
      const hashmap<std::string, std::string>& hierarchies,
      const std::vector<std::string>& subsystems);

  virtual process::Future<std::list<Nothing>> _cleanup(
      const ContainerID& containerId,
      const process::Future<std::list<Nothing>>& future);

  struct Info
  {
    Info(const ContainerID& _containerId, const std::string& _cgroup)
      : containerId(_containerId), cgroup(_cgroup) {}

    const ContainerID containerId;
    const std::string cgroup;
    Option<pid_t> pid;
    Option<Resources> resources;

    process::Promise<mesos::slave::ContainerLimitation> limitation;
  };

  const Flags flags;

  // Map from subsystem to hierarchy.
  hashmap<std::string, std::string> hierarchies;

  // Subsystems used for this isolator. Typically, this is max and current
  std::vector<std::string> subsystems;

  hashmap<ContainerID, Info*> infos;
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __PIDS_ISOLATOR_HPP__
