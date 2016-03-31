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

#include <stdint.h>

#include <mesos/type_utils.hpp>
#include <mesos/values.hpp>

#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/pid.hpp>

#include <stout/bytes.hpp>
#include <stout/check.hpp>
#include <stout/error.hpp>
#include <stout/foreach.hpp>
#include <stout/hashset.hpp>
#include <stout/nothing.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/stringify.hpp>
#include <stout/try.hpp>

#include "linux/cgroups.hpp"

#include "slave/containerizer/mesos/isolators/cgroups/pids.hpp"

using namespace process;

using std::list;
using std::set;
using std::string;
using std::vector;

using mesos::slave::ContainerConfig;
using mesos::slave::ContainerLaunchInfo;
using mesos::slave::ContainerLimitation;
using mesos::slave::ContainerState;
using mesos::slave::Isolator;

namespace mesos {
namespace internal {
namespace slave {

CgroupsPidsIsolatorProcess::CgroupsPidsIsolatorProcess(
    const Flags& _flags,
    const hashmap<string, string>& _hierarchies,
    const vector<string>& _subsystems)
  : flags(_flags),
    hierarchies(_hierarchies),
    subsystems(_subsystems) {}


CgroupsPidsIsolatorProcess::~CgroupsPidsIsolatorProcess() {}


Try<Isolator*> CgroupsPidsIsolatorProcess::create(const Flags& flags)
{
  Try<string> hierarchyPids = cgroups::prepare(
        flags.cgroups_hierarchy,
        "pids",
        flags.cgroups_root);

  if (hierarchyPids.isError()) {
    return Error(
        "Failed to prepare hierarchy for pids subsystem: " +
        hierarchyPids.error());
  }

  hashmap<string, string> hierarchies;
  vector<string> subsystems;

  hierarchies["pids"] = hierarchyPids.get();
  subsystems.push_back("pids");

  // Ensure that no other subsystem is attached to each of the hierarchy.
  Try<set<string>> _subsystems = cgroups::subsystems(hierarchyPids.get());
  if (_subsystems.isError()) {
    return Error(
      "Failed to get the list of attached subsystems for hierarchy " +
      hierarchyPids.get());
  } else if (_subsystems.get().size() != 1) {
    return Error(
      "Unexpected subsystems found attached to the hierarchy " +
      hierarchyPids.get());
  }


  process::Owned<MesosIsolatorProcess> process(
    new CgroupsPidsIsolatorProcess(flags, hierarchies, subsystems));

  return new MesosIsolator(process);
}


Future<Nothing> CgroupsPidsIsolatorProcess::recover(
  const list<ContainerState>& states, const hashset<ContainerID>& orphans)
{
  foreach (const ContainerState& state, states) {
    const ContainerID& containerId = state.container_id();
    const string cgroup = path::join(flags.cgroups_root, containerId.value());

    Try<bool> exists = cgroups::exists(hierarchies["pids"], cgroup);
    if (exists.isError()) {
      foreachvalue (Info* info, infos) {
        delete info;
      }
      infos.clear();
      return Failure(
          "Failed to check cgroup for container " + stringify(containerId));
    }

    if (!exists.get()) {
      // This may occur if the executor has exited and the isolator
      // has destroyed the cgroup but the slave dies before noticing
      // this. This will be detected when the containerizer tries to
      // monitor the executor's pid.
      LOG(WARNING) << "Couldn't find cgroup for container " << containerId;
      continue;
    }

    infos[containerId] = new Info(containerId, cgroup);
  }

  // Remove orphan cgroups.
  foreach (const string& subsystem, subsystems) {
    Try<vector<string>> cgroups = cgroups::get(
        hierarchies[subsystem],
        flags.cgroups_root);

    if (cgroups.isError()) {
      foreachvalue (Info* info, infos) {
        delete info;
      }
      infos.clear();
      return Failure(cgroups.error());
    }

    foreach (const string& cgroup, cgroups.get()) {
      if (cgroup == path::join(flags.cgroups_root, "slave")) {
        continue;
      }

      ContainerID containerId;
      containerId.set_value(Path(cgroup).basename());

      if (infos.contains(containerId)) {
        continue;
      }

      // Known orphan cgroups will be destroyed by the containerizer
      // using the normal cleanup path. See MESOS-2367 for details.
      if (orphans.contains(containerId)) {
        infos[containerId] = new Info(containerId, cgroup);
        continue;
      }

      LOG(INFO) << "Removing unknown orphaned cgroup '"
                << path::join(subsystem, cgroup) << "'";

      // We don't wait on the destroy as we don't want to block recovery.
      cgroups::destroy(
          hierarchies[subsystem],
          cgroup,
          cgroups::DESTROY_TIMEOUT);
    }
  }

  return Nothing();
}


Future<Option<ContainerLaunchInfo>> CgroupsPidsIsolatorProcess::prepare(
  const ContainerID& containerId, const ContainerConfig& containerConfig)
{
  if (infos.contains(containerId)) {
    return Failure("Container has already been prepared");
  }

  // TODO(bmahler): Don't insert into 'infos' unless we create the
  // cgroup successfully. It's safe for now because 'cleanup' gets
  // called if we return a Failure, but cleanup will fail because the
  // cgroup does not exist when cgroups::destroy is called.
  Info* info = new Info(
      containerId, path::join(flags.cgroups_root, containerId.value()));

  infos[containerId] = info;

  foreach (const string& subsystem, subsystems) {
    Try<bool> exists = cgroups::exists(hierarchies[subsystem], info->cgroup);
    if (exists.isError()) {
      return Failure("Failed to prepare isolator: " + exists.error());
    } else if (exists.get()) {
      return Failure("Failed to prepare isolator: cgroup already exists");
    }

    Try<Nothing> create = cgroups::create(hierarchies[subsystem], info->cgroup);
    if (create.isError()) {
      return Failure("Failed to prepare isolator: " + create.error());
    }

    // Chown the cgroup so the executor can create nested cgroups. Do
    // not recurse so the control files are still owned by the slave
    // user and thus cannot be changed by the executor.
    if (containerConfig.has_user()) {
      Try<Nothing> chown = os::chown(
          containerConfig.user(),
          path::join(hierarchies[subsystem], info->cgroup),
          false);
      if (chown.isError()) {
        return Failure("Failed to prepare isolator: " + chown.error());
      }
    }
  }

  return update(containerId, containerConfig.executor_info().resources())
    .then([]() -> Future<Option<ContainerLaunchInfo>> {
      return None();
    });
}


Future<Nothing> CgroupsPidsIsolatorProcess::isolate(
  const ContainerID& containerId, pid_t pid)
{
  if (!infos.contains(containerId)) {
    return Failure("Unknown container");
  }

  Info* info = CHECK_NOTNULL(infos[containerId]);

  CHECK_NONE(info->pid);
  info->pid = pid;

  foreach (const string& subsystem, subsystems) {
    Try<Nothing> assign = cgroups::assign(
        hierarchies[subsystem],
        info->cgroup,
        pid);

    if (assign.isError()) {
      LOG(ERROR) << "Failed to assign container '" << info->containerId
                 << " to its own cgroup '"
                 << path::join(hierarchies[subsystem], info->cgroup)
                 << "' : " << assign.error();

      return Failure("Failed to isolate container: " + assign.error());
    }
  }

  return Nothing();
}


Future<ContainerLimitation> CgroupsPidsIsolatorProcess::watch(
    const ContainerID& containerId)
{
  if (!infos.contains(containerId)) {
    return Failure("Unknown container");
  }

  CHECK_NOTNULL(infos[containerId]);

  return infos[containerId]->limitation.future();
}


Future<Nothing> CgroupsPidsIsolatorProcess::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  const Option<string>& hierarchy = hierarchies.get("pids");
  if (hierarchy.isNone()) {
    return Failure("No 'pids' hierarchy");
  }

  Info* info = CHECK_NOTNULL(infos[containerId]);
  info->resources = resources;

  double pids = resources.pids().get();

  // Set pids.max.
  if (resources.revocable().pids().isSome()) {
    Try<Nothing> write =
      cgroups::pids::max(hierarchy.get(), info->cgroup, PIDS_MAX);

    if (write.isError()) {
      return Failure("Failed to update 'pids.max': " + write.error());
    }

    LOG(INFO) << "Updated 'pids.max' to " << PIDS_MAX << " for container "
              << containerId;
  }

  return Nothing();
}


Future<ResourceStatistics> CgroupsPidsIsolatorProcess::usage(
  const ContainerID& containerId)
{
  if (!infos.contains(containerId)) {
    return Failure("Unknown container");
  }

  return ResourceStatistics();
}


Future<Nothing> CgroupsPidsIsolatorProcess::cleanup(
  const ContainerID& containerId)
{
  // Multiple calls may occur during test clean up.
  if (!infos.contains(containerId)) {
    VLOG(1) << "Ignoring cleanup request for unknown container: "
            << containerId;

    return Nothing();
  }

  Info* info = CHECK_NOTNULL(infos[containerId]);

  list<Future<Nothing>> futures;
  foreach (const string& subsystem, subsystems) {
    futures.push_back(cgroups::destroy(
        hierarchies[subsystem],
        info->cgroup,
        cgroups::DESTROY_TIMEOUT));
  }

  return collect(futures)
    .onAny(
      defer(
        PID<CgroupsPidsIsolatorProcess>(this),
        &CgroupsPidsIsolatorProcess::_cleanup,
        containerId,
        lambda::_1))
    .then([]() { return Nothing(); });
}


Future<list<Nothing>> CgroupsPidsIsolatorProcess::_cleanup(
  const ContainerID& containerId, const Future<list<Nothing>>& future)
{
  if (!infos.contains(containerId)) {
    return Failure("Unknown container");
  }

  CHECK_NOTNULL(infos[containerId]);

  if (!future.isReady()) {
    return Failure(
        "Failed to clean up container " + stringify(containerId) +
        " : " + (future.isFailed() ? future.failure() : "discarded"));
  }

  delete infos[containerId];
  infos.erase(containerId);

  return future;
}

} // namespace slave {
} // namespace internal {
} // namespace mesos {
