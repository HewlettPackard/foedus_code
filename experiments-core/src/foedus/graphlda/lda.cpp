/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
 */
/**
 * @file foedus/graphlda/lda.cpp
 * @brief Latent-Dirichlet Allocation experiment for Topic Modeling
 * @author kimurhid
 * @date 2014/11/22
 * @details
 * This is a port of Fei Chen's LDA topic modeling program to FOEDUS.
 */
#include <stdint.h>
#include <boost/math/special_functions/gamma.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "foedus/engine.hpp"
#include "foedus/engine_options.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/debugging/debugging_supports.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/proc/proc_manager.hpp"
#include "foedus/soc/shared_memory_repo.hpp"
#include "foedus/soc/soc_manager.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/storage_manager.hpp"
#include "foedus/storage/array/array_storage.hpp"
#include "foedus/thread/numa_thread_scope.hpp"
#include "foedus/thread/thread.hpp"
#include "foedus/thread/thread_pool.hpp"
#include "foedus/thread/thread_pool_pimpl.hpp"
#include "foedus/xct/xct_id.hpp"
#include "foedus/xct/xct_manager.hpp"

namespace foedus {
namespace graphlda {

DEFINE_string(dictionary_fname, "dictionary.txt", "Dictionary file");
DEFINE_string(counts_fname, "counts.tsv", "Counts file");
DEFINE_int32(ntopics, 50, "Number of topics");
DEFINE_int32(nburnin, 50, "Number of burnin iterations");
DEFINE_int32(nsamples, 10, "Number of sampling iterations");
DEFINE_int32(nthreads, 16, "Total number of threads");
DEFINE_double(alpha, 1.0, "Alpha prior");
DEFINE_double(beta, 0.1, "Beta prior");
DEFINE_bool(fork_workers, false, "Whether to fork(2) worker threads in child processes rather"
    " than threads in the same process. This is required to scale up to 100+ cores.");
DEFINE_bool(profile, false, "Whether to profile the execution with gperftools.");

typedef uint32_t WordId;
typedef uint32_t DocId;
typedef uint16_t TopicId;
typedef uint32_t Count;
const TopicId kNullTopicId = -1;

/** Used for overflow check */
const Count kUnreasonablyLargeUint32 = (0xFFFF0000U);
inline Count check_overflow(Count count) {
  if (count > kUnreasonablyLargeUint32) {
    return 0;
  } else {
    return count;
  }
}

// storage names
/**
 * n_td(t,d) [1d: d * ntopics + t] Number of occurences of topic t in document d.
 * As an array storage, it's d records of ntopics Count.
 */
const char* kNtd = "n_td";
/**
 * n_tw(t,w) [1d: w * ntopics + t] Number of occurences of word w in topic t.
 * As an array storage, it's w records of ntopics Count.
 */
const char* kNtw = "n_tw";
/**
 * n_t(t) [1d: t] The total number of words assigned to topic t.
 * As an array storage, it's 1 record of ntopics Count.
 */
const char* kNt = "n_t";

struct Token {
  WordId word;
  DocId doc;
  Token(WordId word = 0, DocId doc = 0) : word(word), doc(doc) { }
};
std::ostream& operator<<(std::ostream& out, const Token& tok) {
  return out << "(" << tok.word << ", " << tok.doc << ")";
}

/** This object is held only by the driver thread. Individual worker uses SharedInputs. */
struct Corpus {
  size_t nwords, ndocs, ntokens;
  std::vector< Token > tokens;
  std::vector<std::string> dictionary;

  Corpus(const fs::Path& dictionary_fname, const fs::Path& counts_fname)
    : nwords(0), ndocs(0), ntokens(0) {
    dictionary.reserve(20000);
    tokens.reserve(100000);
    load_dictionary(dictionary_fname);
    load_counts(counts_fname);
  }
  void load_dictionary(const fs::Path& fname) {
    std::ifstream fin(fname.c_str());
    std::string str;
    while (fin.good()) {
      std::getline(fin, str);
      if (fin.good()) {
        dictionary.push_back(str);
        nwords++;
      }
    }
    fin.close();
  }

  void load_counts(const fs::Path& fname)  {
    std::ifstream fin(fname.c_str());
    while (fin.good()) {
      // Read a collection of tokens
      const size_t NULL_VALUE(-1);
      size_t word = NULL_VALUE, doc = NULL_VALUE, count = NULL_VALUE;
      fin >> doc >> word >> count;
      if (fin.good()) {
        ASSERT_ND(word != NULL_VALUE && doc != NULL_VALUE && count != NULL_VALUE);
        // update the doc counter
        ndocs = std::max(ndocs, doc + 1);
        // Assert valid word
        ASSERT_ND(word < nwords);
        // Update the words in document counter
        // Add all the tokens
        Token tok; tok.word = word; tok.doc = doc;
        for (size_t i = 0; i < count; ++i) {
            tokens.push_back(tok);
        }
      }
    }
    fin.close();
    ntokens = tokens.size();
  }
};

/** A copy of essential parts of Corpus, which can be placed in shared memory. No vector/string */
struct SharedInputs {
  uint32_t  nwords;
  uint32_t  ndocs;
  uint32_t  ntokens;
  uint32_t  ntopics;
  uint32_t  niterations;
  double    alpha;
  double    beta;
  /**
   * Actually "Token tokens[ntokens]" and then "TopicId topics[ntokens]".
   * Note that you can't do sizeof(SharedCorpus).
   */
  char      dynamic_data[8];

  static uint64_t get_object_size(uint32_t ntokens) {
    return sizeof(SharedInputs) + (sizeof(Token) + sizeof(TopicId)) * ntokens;
  }

  void init(const Corpus& original) {
    nwords = original.nwords;
    ndocs = original.ndocs;
    ntokens = original.ntokens;
    ntopics = FLAGS_ntopics;
    niterations = 0;  // set separately
    alpha = FLAGS_alpha;
    beta = FLAGS_beta;
    std::memcpy(get_tokens_data(), &(original.tokens[0]), sizeof(Token) * ntokens);
    std::memset(get_topics_data(), 0xFFU, sizeof(TopicId) * ntokens);  // null topic ID
  }

  Token* get_tokens_data() {
    return reinterpret_cast<Token*>(dynamic_data);
  }
  TopicId* get_topics_data() {
    return reinterpret_cast<TopicId*>(dynamic_data + sizeof(Token) * ntokens);
  }
};

/** Core implementation of LDA experiment. */
class LdaWorker {
 public:
  enum Constant {
    kIntNormalizer = 1 << 24,
    kNtApplyBatch = 1 << 8,
  };
  explicit LdaWorker(const proc::ProcArguments& args) {
    engine = args.engine_;
    context = args.context_;
    shared_inputs = reinterpret_cast<SharedInputs*>(
      engine->get_soc_manager()->get_shared_memory_repo()->get_global_user_memory());
    *args.output_used_ = 0;
  }
  ErrorStack on_lda_worker_task() {
    uint16_t thread_id = context->get_thread_global_ordinal();
    LOG(INFO) << "Thread-" << thread_id << " started";
    unirand.set_current_seed(thread_id);


    Token* shared_tokens = shared_inputs->get_tokens_data();
    ntopics = shared_inputs->ntopics;
    ntopics_aligned = assorted::align8(ntopics);
    numwords = shared_inputs->nwords;

    TopicId* shared_topics = shared_inputs->get_topics_data();
    const uint32_t numtokens = shared_inputs->ntokens;
    const uint32_t niterations = shared_inputs->niterations;
    const uint16_t all_threads = engine->get_options().thread_.get_total_thread_count();
    tokens_per_core = numtokens / all_threads;
    int_a = static_cast<uint64_t>(shared_inputs->alpha * kIntNormalizer);
    int_b = static_cast<uint64_t>(shared_inputs->beta * kIntNormalizer);

    // allocate tmp memory on local NUMA node.
    // These are the only memory that need dynamic allocation in main_loop
    uint64_t tmp_memory_size = ntopics_aligned * sizeof(uint64_t)  // conditional
        + tokens_per_core * sizeof(Token)  // assigned_tokens
        + ntopics_aligned * sizeof(Count)  // record_td
        + ntopics_aligned * sizeof(Count)  // record_tw
        + ntopics_aligned * sizeof(Count)  // record_t
        + ntopics_aligned * sizeof(Count)  // record_t_diff
        + tokens_per_core * sizeof(TopicId);  // topics_tmp
    tmp_memory.alloc(
      tmp_memory_size,
      1 << 21,
      memory::AlignedMemory::kNumaAllocOnnode,
      context->get_numa_node());
    char* tmp_block = reinterpret_cast<char*>(tmp_memory.get_block());

    int_conditional = reinterpret_cast<uint64_t*>(tmp_block);
    tmp_block += ntopics_aligned * sizeof(uint64_t);

    assigned_tokens = reinterpret_cast<Token*>(tmp_block);
    tmp_block += tokens_per_core * sizeof(Token);

    record_td = reinterpret_cast<Count*>(tmp_block);
    tmp_block += ntopics_aligned * sizeof(Count);

    record_tw = reinterpret_cast<Count*>(tmp_block);
    tmp_block += ntopics_aligned * sizeof(Count);

    record_t = reinterpret_cast<Count*>(tmp_block);
    tmp_block += ntopics_aligned * sizeof(Count);

    record_t_diff = reinterpret_cast<Count*>(tmp_block);
    tmp_block += ntopics_aligned * sizeof(Count);

    topics_tmp = reinterpret_cast<TopicId*>(tmp_block);
    tmp_block += tokens_per_core * sizeof(TopicId);

    uint64_t size_check = tmp_block - reinterpret_cast<char*>(tmp_memory.get_block());
    ASSERT_ND(size_check == tmp_memory_size);

    // initialize with previous result (if this is burnin, all null-topic)
    uint64_t assign_pos = tokens_per_core * thread_id;
    std::memcpy(topics_tmp, shared_topics + assign_pos, tokens_per_core * sizeof(TopicId));
    std::memcpy(assigned_tokens, shared_tokens + assign_pos, tokens_per_core * sizeof(Token));

    n_td = storage::array::ArrayStorage(engine, kNtd);
    n_tw = storage::array::ArrayStorage(engine, kNtw);
    n_t = storage::array::ArrayStorage(engine, kNt);
    ASSERT_ND(n_td.exists());
    ASSERT_ND(n_tw.exists());
    ASSERT_ND(n_t.exists());

    nchanges = 0;
    std::memset(int_conditional, 0, sizeof(uint64_t) * ntopics_aligned);
    std::memset(record_td, 0, sizeof(Count) * ntopics_aligned);
    std::memset(record_tw, 0, sizeof(Count) * ntopics_aligned);
    std::memset(record_t, 0, sizeof(Count) * ntopics_aligned);
    std::memset(record_t_diff, 0, sizeof(Count) * ntopics_aligned);

    // Loop over all the tokens
    for (size_t gn = 0; gn < niterations; ++gn) {
      LOG(INFO) << "Thread-" << thread_id << " iteration " << gn << "/" << niterations;
      WRAP_ERROR_CODE(main_loop());
    }

    // copy back the topics to shared. it's per-core, so this is the only thread that modifies it.
    std::memcpy(shared_topics + assign_pos, topics_tmp, tokens_per_core * sizeof(TopicId));

    const uint64_t ntotal = niterations * tokens_per_core;
    LOG(INFO) << "Thread-" << thread_id << " done. nchanges/ntotal="
      << nchanges << "/" << ntotal << "=" << (static_cast<double>(nchanges) / ntotal);
    return kRetOk;
  }

 private:
  Engine* engine;
  thread::Thread* context;
  SharedInputs* shared_inputs;
  uint16_t ntopics;
  uint16_t ntopics_aligned;
  uint32_t numwords;
  uint32_t tokens_per_core;
  uint64_t nchanges;
  uint64_t int_a;
  uint64_t int_b;
  storage::array::ArrayStorage n_td;  // d records of ntopics Count
  storage::array::ArrayStorage n_tw;  // w records of ntopics Count
  storage::array::ArrayStorage n_t;  // 1 record of ntopics Count
  assorted::UniformRandom unirand;

  /** local memory to back the followings */
  memory::AlignedMemory tmp_memory;
  uint64_t* int_conditional;  // [ntopics]
  Token* assigned_tokens;  // [tokens_per_core]
  Count* record_td;  // [ntopics]
  Count* record_tw;  // [ntopics]
  Count* record_t;  // [ntopics]
  Count* record_t_diff;  // [ntopics]. This batches changes to n_t, which is so contentious.
  TopicId* topics_tmp;  // [tokens_per_core]

  ErrorCode main_loop() {
    xct::XctManager* xct_manager = engine->get_xct_manager();
    uint16_t batched_t = 0;
    for (size_t i = 0; i < tokens_per_core; ++i) {
      // Get the word and document for the ith token
      const WordId w = assigned_tokens[i].word;
      const DocId d = assigned_tokens[i].doc;
      const TopicId old_topic = topics_tmp[i];

      CHECK_ERROR_CODE(xct_manager->begin_xct(context, xct::kDirtyReadPreferVolatile));

      // First, read from the global arrays
      CHECK_ERROR_CODE(n_td.get_record(context, d, record_td));
      CHECK_ERROR_CODE(n_tw.get_record(context, w, record_tw));

      // Construct the conditional
      uint64_t normalizer = 0;
      // We do the following calculation without double/float to speed up.
      // Notation: int_x := x * N (eg, int_ntd := ntd * N, int_alpha = alpha * N)
      //           where N is some big number, such as 2^24
      //      conditional[t] = (a + ntd) * (b + ntw) / (b * nwords + nt)
      // <=>  conditional[t] = (int_a + int_ntd) * (b + ntw) / (int_b * nwords + int_nt)
      // <=>  int_conditional[t] = (int_a + int_ntd) * (int_b + int_ntw) / (int_b * nwords + int_nt)
      for (TopicId t = 0; t < ntopics; ++t) {
        uint64_t int_ntd = record_td[t], int_ntw = record_tw[t], int_nt = record_t[t];
        if (t == old_topic) {
          // locally apply the decrements. we apply it to global array
          // when we are sure that new_topic != old_topic
          --int_ntd;
          --int_ntw;
          --int_nt;
        }
        int_ntd = check_overflow(int_ntd) * kIntNormalizer;
        int_ntw = check_overflow(int_ntw) * kIntNormalizer;
        int_nt = check_overflow(int_nt) * kIntNormalizer;

        int_conditional[t] = (int_a + int_ntd) * (int_b + int_ntw) / (int_b * numwords + int_nt);
        normalizer += int_conditional[t];
      }

      // Draw a new valuez
      TopicId new_topic = topic_multinomial(normalizer);

      // Update the topic assignment and counters
      if (new_topic != old_topic) {
        nchanges++;
        topics_tmp[i] = new_topic;

        // We apply the inc/dec to global array only in this case.
        if (old_topic != kNullTopicId) {
          // Remove the word from the old counters
          uint16_t offset = old_topic * sizeof(Count);
          CHECK_ERROR_CODE(n_td.increment_record_oneshot<Count>(context, d, -1, offset));
          CHECK_ERROR_CODE(n_tw.increment_record_oneshot<Count>(context, w, -1, offset));
          // change to t are batched. will be applied later
          --record_t[old_topic];
          --record_t_diff[old_topic];
        }

        // Add the word to the new counters
        uint16_t offset = new_topic * sizeof(Count);
        CHECK_ERROR_CODE(n_td.increment_record_oneshot<Count>(context, d, 1, offset));
        CHECK_ERROR_CODE(n_tw.increment_record_oneshot<Count>(context, w, 1, offset));
        ++record_t[new_topic];
        ++record_t_diff[new_topic];

        Epoch ep;
        // because this is not serializable level and the only update is one-shot increment,
        // no race is possible.
        CHECK_ERROR_CODE(xct_manager->precommit_xct(context, &ep));

        ++batched_t;
        if (batched_t >= kNtApplyBatch) {
          CHECK_ERROR_CODE(sync_record_t());
          batched_t = 0;
        }
      } else {
        CHECK_ERROR_CODE(xct_manager->abort_xct(context));
      }
    }

    CHECK_ERROR_CODE(sync_record_t());

    return kErrorCodeOk;
  }

  TopicId topic_multinomial(uint64_t normalizer) {
    uint64_t sample = unirand.next_uint64() % normalizer;
    uint64_t cur_sum = 0;
    for (TopicId t = 0; t < ntopics; ++t) {
      cur_sum += int_conditional[t];
      if (sample <= cur_sum) {
        return t;
      }
    }
    return ntopics - 1U;
  }


  ErrorCode sync_record_t() {
    xct::XctManager* xct_manager = engine->get_xct_manager();
    Epoch ep;
    // first, "flush" batched increments/decrements to the global n_t
    // this transactionally applies all the inc/dec.
    CHECK_ERROR_CODE(xct_manager->begin_xct(context, xct::kDirtyReadPreferVolatile));
    for (TopicId t = 0; t < ntopics_aligned; t += 4) {
      // for each uint64_t. we reserved additional size (align8) for simplifying this.
      uint64_t diff = *reinterpret_cast<uint64_t*>(&record_t_diff[t]);
      if (diff != 0) {
        uint16_t offset = t * sizeof(Count);
        CHECK_ERROR_CODE(n_t.increment_record_oneshot<uint64_t>(context, 0, diff, offset));
      }
    }
    CHECK_ERROR_CODE(xct_manager->precommit_xct(context, &ep));

    // then, retrive a fresh new copy of record_t, which contains updates from other threads.
    std::memset(record_t, 0, sizeof(Count) * ntopics_aligned);
    std::memset(record_t_diff, 0, sizeof(Count) * ntopics_aligned);
    CHECK_ERROR_CODE(xct_manager->begin_xct(context, xct::kDirtyReadPreferVolatile));
    CHECK_ERROR_CODE(n_t.get_record(context, 0, record_t));
    CHECK_ERROR_CODE(xct_manager->abort_xct(context));
    return kErrorCodeOk;
  }
};

ErrorStack lda_worker_task(const proc::ProcArguments& args) {
  LdaWorker worker(args);
  return worker.on_lda_worker_task();
}

void run_workers(Engine* engine, SharedInputs* shared_inputs, uint32_t niterations) {
  const EngineOptions& options = engine->get_options();

  auto* thread_pool = engine->get_thread_pool();
  std::vector< thread::ImpersonateSession > sessions;

  shared_inputs->niterations = niterations;
  for (uint16_t node = 0; node < options.thread_.group_count_; ++node) {
    for (uint16_t ordinal = 0; ordinal < options.thread_.thread_count_per_group_; ++ordinal) {
      thread::ImpersonateSession session;
      bool ret = thread_pool->impersonate_on_numa_node(
        node,
        "worker_task",
        nullptr,
        0,
        &session);
      if (!ret) {
        LOG(FATAL) << "Couldn't impersonate";
      }
      sessions.emplace_back(std::move(session));
    }
  }
  LOG(INFO) << "Started running! #iter=" << niterations;
  for (uint32_t i = 0; i < sessions.size(); ++i) {
    LOG(INFO) << "result[" << i << "]=" << sessions[i].get_result();
    COERCE_ERROR(sessions[i].get_result());
    sessions[i].release();
  }
  LOG(INFO) << "Completed";
}

ErrorStack lda_populate_array_task(const proc::ProcArguments& args) {
  ASSERT_ND(args.input_len_ == sizeof(storage::StorageId));
  storage::StorageId storage_id = *reinterpret_cast<const storage::StorageId*>(args.input_buffer_);
  storage::array::ArrayStorage array(args.engine_, storage_id);
  ASSERT_ND(array.exists());
  thread::Thread* context = args.context_;

  uint16_t nodes = args.engine_->get_options().thread_.group_count_;
  uint16_t node = context->get_numa_node();
  uint64_t records_per_node = array.get_array_size() / nodes;
  storage::array::ArrayOffset begin = records_per_node * node;
  storage::array::ArrayOffset end = records_per_node * (node + 1U);
  WRAP_ERROR_CODE(array.prefetch_pages(context, true, false, begin, end));

  LOG(INFO) << "Population of " << array.get_name() << " done in node-" << node;
  return kRetOk;
}

void create_count_array(
  Engine* engine,
  const char* name,
  uint64_t records,
  uint64_t counts) {
  Epoch ep;
  uint64_t counts_aligned = assorted::align8(counts);
  storage::array::ArrayMetadata meta(name, counts_aligned * sizeof(Count), records);
  COERCE_ERROR(engine->get_storage_manager()->create_storage(&meta, &ep));
  LOG(INFO) << "Populating empty " << name << "(id=" << meta.id_ << ")...";

  // populate it with one thread per node.
  const EngineOptions& options = engine->get_options();
  auto* thread_pool = engine->get_thread_pool();
  std::vector< thread::ImpersonateSession > sessions;
  for (uint16_t node = 0; node < options.thread_.group_count_; ++node) {
    thread::ImpersonateSession session;
    bool ret = thread_pool->impersonate_on_numa_node(
      node,
      "populate_array_task",
      &meta.id_,
      sizeof(meta.id_),
      &session);
    if (!ret) {
      LOG(FATAL) << "Couldn't impersonate";
    }
    sessions.emplace_back(std::move(session));
  }
  for (uint32_t i = 0; i < sessions.size(); ++i) {
    LOG(INFO) << "populate result[" << i << "]=" << sessions[i].get_result();
    COERCE_ERROR(sessions[i].get_result());
    sessions[i].release();
  }
  LOG(INFO) << "Populated empty " << name;
}

double run_experiment(Engine* engine, const Corpus &corpus) {
  const EngineOptions& options = engine->get_options();

  // Input information placed in shared memory
  SharedInputs* shared_inputs = reinterpret_cast<SharedInputs*>(
    engine->get_soc_manager()->get_shared_memory_repo()->get_global_user_memory());
  ASSERT_ND(options.soc_.shared_user_memory_size_kb_ * (1ULL << 10)
    >= SharedInputs::get_object_size(corpus.ntokens));
  shared_inputs->init(corpus);

  // create empty tables.
  create_count_array(engine, kNtd, corpus.ndocs, FLAGS_ntopics);
  create_count_array(engine, kNtw, corpus.nwords, FLAGS_ntopics);
  create_count_array(engine, kNt, 1, FLAGS_ntopics);

  LOG(INFO) << "Created arrays";

  if (FLAGS_profile) {
    COERCE_ERROR(engine->get_debug()->start_profile("lda.prof"));
  }

  debugging::StopWatch watch;
  run_workers(engine, shared_inputs, FLAGS_nburnin);
  LOG(INFO) << "Completed burnin!";
  run_workers(engine, shared_inputs, FLAGS_nsamples);
  LOG(INFO) << "Completed final sampling!";

  watch.stop();

  LOG(INFO) << "Experiment ended. Elapsed time=" << watch.elapsed_sec() << "sec";
  if (FLAGS_profile) {
    engine->get_debug()->stop_profile();
    LOG(INFO) << "Check out the profile result: pprof --pdf lda lda.prof > lda.pdf; okular lda.pdf";
  }

  LOG(INFO) << "Shutting down...";

  LOG(INFO) << engine->get_memory_manager()->dump_free_memory_stat();
  return watch.elapsed_sec();
}


int driver_main(int argc, char** argv) {
  gflags::SetUsageMessage("LDA sampler code");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  fs::Path dictionary_fname(FLAGS_dictionary_fname);
  fs::Path counts_fname(FLAGS_counts_fname);

  if (!fs::exists(dictionary_fname)) {
    std::cerr << "The dictionary file doesn't exist: " << dictionary_fname;
    return 1;
  } else if (!fs::exists(counts_fname)) {
    std::cerr << "The count file doesn't exist: " << counts_fname;
    return 1;
  }

  fs::Path folder("/dev/shm/foedus_lda");
  if (fs::exists(folder)) {
    fs::remove_all(folder);
  }
  if (!fs::create_directories(folder)) {
    std::cerr << "Couldn't create " << folder << ". err=" << assorted::os_error();
    return 1;
  }

  EngineOptions options;

  fs::Path savepoint_path(folder);
  savepoint_path /= "savepoint.xml";
  options.savepoint_.savepoint_path_.assign(savepoint_path.string());
  ASSERT_ND(!fs::exists(savepoint_path));

  std::cout << "NUMA node count=" << static_cast<int>(options.thread_.group_count_) << std::endl;
  uint16_t thread_count = FLAGS_nthreads;
  if (thread_count < options.thread_.group_count_) {
    std::cout << "nthreads less than socket count. Using subset of sockets" << std::endl;
    options.thread_.group_count_ = thread_count;
    options.thread_.thread_count_per_group_ = 1;
  } else if (thread_count % options.thread_.group_count_ != 0) {
    std::cout << "nthreads not multiply of #sockets. adjusting" << std::endl;
    options.thread_.thread_count_per_group_ = thread_count / options.thread_.group_count_;
  } else {
    options.thread_.thread_count_per_group_ = thread_count / options.thread_.group_count_;
  }

  options.snapshot_.folder_path_pattern_ = "/dev/shm/foedus_lda/snapshot/node_$NODE$";
  options.snapshot_.snapshot_interval_milliseconds_ = 100000000U;

  options.log_.folder_path_pattern_ = "/dev/shm/foedus_lda/log/node_$NODE$/logger_$LOGGER$";
  options.log_.loggers_per_node_ = 1;
  options.log_.flush_at_shutdown_ = false;
  options.log_.log_file_size_mb_ = 1 << 16;
  options.log_.emulation_.null_device_ = true;

  options.memory_.page_pool_size_mb_per_node_ = 1 << 8;
  options.cache_.snapshot_cache_size_mb_per_node_ = 1 << 6;

  if (FLAGS_fork_workers) {
    std::cout << "Will fork workers in child processes" << std::endl;
    options.soc_.soc_type_ = kChildForked;
  }

  options.debugging_.debug_log_min_threshold_
    = debugging::DebuggingOptions::kDebugLogInfo;
    // = debugging::DebuggingOptions::kDebugLogWarning;
  options.debugging_.verbose_modules_ = "";
  options.debugging_.verbose_log_level_ = -1;

  std::cout << "Loading the corpus." << std::endl;
  Corpus corpus(dictionary_fname, counts_fname);

  std::cout << "Number of words:   " << corpus.nwords << std::endl
            << "Number of docs:    " << corpus.ndocs << std::endl
            << "Number of tokens:  " << corpus.ntokens << std::endl
            << "Number of topics:  " << FLAGS_ntopics << std::endl
            << "Number of threads: " << options.thread_.get_total_thread_count() << std::endl
            << "Alpha:             " << FLAGS_alpha   << std::endl
            << "Beta:              " << FLAGS_beta    << std::endl;

  options.soc_.shared_user_memory_size_kb_
    = (SharedInputs::get_object_size(corpus.ntokens) >> 10) + 64;

  double elapsed;
  {
    Engine engine(options);
    engine.get_proc_manager()->pre_register("worker_task", lda_worker_task);
    engine.get_proc_manager()->pre_register("populate_array_task", lda_populate_array_task);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      elapsed = run_experiment(&engine, corpus);
      COERCE_ERROR(engine.uninitialize());
    }
  }

  std::cout << "All done. elapsed time=" << elapsed << "sec" << std::endl;

  return 0;
}

}  // namespace graphlda
}  // namespace foedus

int main(int argc, char **argv) {
  return foedus::graphlda::driver_main(argc, argv);
}

