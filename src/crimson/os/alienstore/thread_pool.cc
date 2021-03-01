#include "thread_pool.h"

#include <chrono>
#include <pthread.h>

#include "include/ceph_assert.h"
#include "crimson/common/config_proxy.h"

using crimson::common::local_conf;

namespace crimson::os {

ThreadPool::ThreadPool(size_t n_threads,
                       size_t queue_sz,
                       long cpu_id)
  : n_threads(n_threads),
    mutexes(n_threads),
    conds(n_threads),
    queue_size{round_up_to(queue_sz, seastar::smp::count)},
    cpu_id(cpu_id)
{
  for (size_t i = 0; i < n_threads; i++) {
    pendings.emplace_back(queue_size);
  }
}

ThreadPool::~ThreadPool()
{
  for (auto& thread : threads) {
    thread.join();
  }
}

void ThreadPool::pin(unsigned cpu_id)
{
  cpu_set_t cs;
  CPU_ZERO(&cs);
  CPU_SET(cpu_id, &cs);
  [[maybe_unused]] auto r = pthread_setaffinity_np(pthread_self(),
                                                   sizeof(cs), &cs);
  ceph_assert(r == 0);
}

void ThreadPool::loop(std::chrono::milliseconds queue_max_wait, size_t shard)
{
  auto& mutex = mutexes[shard];
  auto& cond = conds[shard];
  auto& pending = this->pendings[shard];
  for (;;) {
    WorkItem* work_item = nullptr;
    {
      std::unique_lock lock{mutex};
      cond.wait_for(lock, queue_max_wait,
		    [this, &work_item, &pending] {
	bool empty = true;
	if (!pending.empty()) {
	  empty = false;
	  work_item = pending.front();
	  pending.pop_front();
	}
        return !empty || is_stopping();
      });
      //while (!pending[shard].pop(work_item) && !is_stopping())
	//::usleep(100);
    }

    if (work_item) {
      //::crimson::get_logger(ceph_subsys_filestore).info("threadpool loop, concurrent_ops: {}", ++concurrent_ops);
      work_item->process();
      //--concurrent_ops;
    } else if (is_stopping()) {
      break;
    }
  }
}

seastar::future<> ThreadPool::start()
{
  auto queue_max_wait = std::chrono::seconds(local_conf()->threadpool_empty_queue_max_wait);
  auto slots_per_shard = queue_size / seastar::smp::count;
  for (auto [i, cpu] = std::tuple{(size_t)0, cpu_id}; i < n_threads; i++, cpu--) {
    threads.emplace_back([this, queue_max_wait, i, cpu] {
      if (cpu >= 0) {
        pin(cpu);
      }
      (void) pthread_setname_np(pthread_self(), "alien-store-tp");
      loop(queue_max_wait, i);
    });
  }

  return submit_queue.start(slots_per_shard);
}

seastar::future<> ThreadPool::stop()
{
  return submit_queue.stop().then([this] {
    stopping = true;
    for (auto& cond : conds)
      cond.notify_all();
  });
}

} // namespace crimson::os
