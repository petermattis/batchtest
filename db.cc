#include <condition_variable>
#include <mutex>
#include <vector>
#include <unistd.h>
#include "db.h"

struct DB {
  std::mutex mu;
  std::condition_variable cond;
  bool committing;
  uint64_t commit_seq;
  uint64_t pending_seq;
  std::vector<Batch*> pending;

  DB()
      : committing(false),
        commit_seq(0),
        pending_seq(0) {
  }
};

struct Batch {
  DB* const db;

  Batch(DB *d)
      : db(d) {
  }
};

DB* NewDB() {
  return new DB;
}

Batch* NewBatch(DB *db) {
  return new Batch(db);
}

void FreeBatch(Batch *batch) {
  delete batch;
}

int CommitBatch(DB* db, Batch* batch) {
  std::unique_lock<std::mutex> guard(db->mu);
  const bool leader = db->pending.empty();
  const uint64_t seq = db->pending_seq;
  db->pending.push_back(batch);
  int size = 0;

  if (leader) {
    // We're the leader. Wait for any running commit to finish.
    db->cond.wait(guard, [db] {
        return !db->committing;
      });
    std::vector<Batch*> pending;
    pending.swap(db->pending);
    db->pending_seq++;
    db->committing = true;
    db->mu.unlock();

    for (int i = 1; i < pending.size(); ++i) {
      // CombineBatch(batch, pending[i]);
    }

    ApplyBatch(batch);
    size = int(pending.size());

    db->mu.lock();
    db->committing = false;
    db->commit_seq = seq;
    db->cond.notify_all();
  } else {
    // We're a follower. Wait for the commit to finish.
    db->cond.wait(guard, [db, seq] {
        return db->commit_seq >= seq;
      });
  }
  return size;
}

void ApplyBatch(Batch* batch) {
  usleep(5000 /* 5 ms */);
}
