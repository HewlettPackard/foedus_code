/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
DEFINE_int32(nburnin, 5, "Number of burnin iterations");
DEFINE_int32(nsamples, 1, "Number of sampling iterations");
DEFINE_int32(nthreads, 1, "Total number of threads");
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

/** n_td(t,d) [1d: d * ntopics + t] Number of occurences of topic t in document d */
const char* kNtd = "n_td";
/** n_tw(t,w) [1d: w * ntopics + t] Number of occurences of word w in topic t */
const char* kNtw = "n_tw";
/** n_t(t) [1d: t] The total number of words assigned to topic t */
const char* kNt = "n_t";

/** d * ntopics + t */
inline uint32_t to_td(TopicId t, DocId d, uint32_t ntopics) { return d * ntopics + t; }
/** w * ntopics + t */
inline uint32_t to_tw(TopicId t, WordId w, uint32_t ntopics) { return w * ntopics + t; }

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
   * Actually Token tokens[ntokens] and then TopicId topics[ntokens].
   * Note that you can't do sizeof(SharedCorpus).
   */
  char      dynamic_data[8];

  static uint64_t get_object_size(uint32_t ntokens) {
    return sizeof(uint32_t) * 4 + sizeof(double) * 2 + (sizeof(Token) + sizeof(TopicId)) * ntokens;
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

class LdaWorker {
 public:
  enum Constant {
    kIntNormalizer = 1 << 24,
  };
  explicit LdaWorker(const proc::ProcArguments& args) {
    // Maybe we want to copy this input to a local memory.
    engine = args.engine_;
    context = args.context_;
    shared_inputs = reinterpret_cast<SharedInputs*>(
      engine->get_soc_manager()->get_shared_memory_repo()->get_global_user_memory());
    *args.output_used_ = 0;
  }
  ErrorStack on_lda_worker_task() {
    thread_id = context->get_thread_global_ordinal();
    LOG(INFO) << "Thread-" << thread_id << " started";
    unirand.set_current_seed(thread_id);


    shared_tokens = shared_inputs->get_tokens_data();
    ntopics = shared_inputs->ntopics;
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
    uint64_t tmp_memory_size = ntopics * sizeof(uint64_t)  // conditional
        + ntopics * sizeof(storage::array::ArrayOffset)  // batch_array_in
        + ntopics * sizeof(Count)  // batch_array_td
        + ntopics * sizeof(Count)  // batch_array_tw
        + ntopics * sizeof(Count)  // batch_array_t
        + tokens_per_core * sizeof(TopicId);  // topics_tmp
    tmp_memory.alloc(
      tmp_memory_size,
      1 << 21,
      memory::AlignedMemory::kNumaAllocOnnode,
      context->get_numa_node());
    char* tmp_block = reinterpret_cast<char*>(tmp_memory.get_block());

    int_conditional = reinterpret_cast<uint64_t*>(tmp_block);
    tmp_block += ntopics * sizeof(uint64_t);

    batch_in = reinterpret_cast<storage::array::ArrayOffset*>(tmp_block);
    tmp_block += ntopics * sizeof(storage::array::ArrayOffset);

    batch_td = reinterpret_cast<Count*>(tmp_block);
    tmp_block += ntopics * sizeof(Count);

    batch_tw = reinterpret_cast<Count*>(tmp_block);
    tmp_block += ntopics * sizeof(Count);

    batch_t = reinterpret_cast<Count*>(tmp_block);
    tmp_block += ntopics * sizeof(Count);

    topics_tmp = reinterpret_cast<TopicId*>(tmp_block);
    tmp_block += tokens_per_core * sizeof(TopicId);

    uint64_t size_check = tmp_block - reinterpret_cast<char*>(tmp_memory.get_block());
    ASSERT_ND(size_check == tmp_memory_size);

    // initialize with previous result (of this is burnin, all null-topic)
    std::memcpy(
      topics_tmp,
      shared_topics + tokens_per_core * thread_id,
      tokens_per_core * sizeof(TopicId));

    n_td = storage::array::ArrayStorage(engine, kNtd);
    n_tw = storage::array::ArrayStorage(engine, kNtw);
    n_t = storage::array::ArrayStorage(engine, kNt);
    ASSERT_ND(n_td.exists());
    ASSERT_ND(n_tw.exists());
    ASSERT_ND(n_t.exists());

    nchanges = 0;

    // Loop over all the tokens
    for (size_t gn = 0; gn < niterations; ++gn) {
      LOG(INFO) << "Thread-" << thread_id << " iteration " << gn << "/" << niterations;
      WRAP_ERROR_CODE(main_loop());
    }

    // copy back the topics to shared. it's per-core, so this is the only thread that modifies it.
    std::memcpy(
      shared_topics + tokens_per_core * thread_id,
      topics_tmp,
      tokens_per_core * sizeof(TopicId));

    const uint64_t ntotal = niterations * tokens_per_core;
    LOG(INFO) << "Thread-" << thread_id << " done. nchanges/ntotal="
      << nchanges << "/" << ntotal << "=" << (static_cast<double>(nchanges) / ntotal);
    return kRetOk;
  }

 private:
  Engine* engine;
  thread::Thread* context;
  uint16_t thread_id;
  SharedInputs* shared_inputs;
  Token* shared_tokens;
  uint16_t ntopics;
  uint32_t numwords;
  uint32_t tokens_per_core;
  uint64_t nchanges;
  uint64_t int_a;
  uint64_t int_b;
  storage::array::ArrayStorage n_td;
  storage::array::ArrayStorage n_tw;
  storage::array::ArrayStorage n_t;
  assorted::UniformRandom unirand;

  /** local memory to back the followings */
  memory::AlignedMemory tmp_memory;
  uint64_t* int_conditional;  // [ntopics]
  storage::array::ArrayOffset* batch_in;  // [ntopics]
  Count* batch_td;  // [ntopics]
  Count* batch_tw;  // [ntopics]
  Count* batch_t;  // [ntopics]
  TopicId* topics_tmp;  // [tokens_per_core]

  ErrorCode main_loop() {
    xct::XctManager* xct_manager = engine->get_xct_manager();
    for (size_t i = 0; i < tokens_per_core; ++i) {
      const uint32_t token_id = i + tokens_per_core * thread_id;
      // Get the word and document for the ith token
      const WordId w = shared_tokens[token_id].word;
      const DocId d = shared_tokens[token_id].doc;
      const TopicId old_topic = topics_tmp[i];

      CHECK_ERROR_CODE(xct_manager->begin_xct(context, xct::kDirtyReadPreferVolatile));

      // First, read from the global arrays in batch
      // n_td(t,d) [1d: d * ntopics + t] Number of occurences of topic t in document d
      for (TopicId t = 0; t < ntopics; ++t) {
        batch_in[t] = d * ntopics + t;
      }
      CHECK_ERROR_CODE(n_td.get_record_primitive_batch(context, 0, ntopics, batch_in, batch_td));

      // n_tw(t,w) [1d: w * ntopics + t] Number of occurences of word w in topic t
      for (TopicId t = 0; t < ntopics; ++t) {
        batch_in[t] = w * ntopics + t;
      }
      CHECK_ERROR_CODE(n_tw.get_record_primitive_batch(context, 0, ntopics, batch_in, batch_tw));

      // n_t(t) [1d: t] The total number of words assigned to topic t
      for (TopicId t = 0; t < ntopics; ++t) {
        batch_in[t] = t;
      }
      CHECK_ERROR_CODE(n_t.get_record_primitive_batch(context, 0, ntopics, batch_in, batch_t));

      // Construct the conditional
      uint64_t normalizer = 0;
      // We do the following calculation without double/float to speed up.
      // Notation: int_x := x * N (eg, int_ntd := ntd * N, int_alpha = alpha * N)
      //           where N is some big number, such as 2^24
      //      conditional[t] = (a + ntd) * (b + ntw) / (b * nwords + nt)
      // <=>  conditional[t] = (int_a + int_ntd) * (b + ntw) / (int_b * nwords + int_nt)
      // <=>  int_conditional[t] = (int_a + int_ntd) * (int_b + int_ntw) / (int_b * nwords + int_nt)
      for (TopicId t = 0; t < ntopics; ++t) {
        uint64_t int_ntd = batch_td[t], int_ntw = batch_tw[t], int_nt = batch_t[t];
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
          uint32_t old_td = to_td(old_topic, d, ntopics);
          CHECK_ERROR_CODE(n_td.increment_record_oneshot<uint32_t>(context, old_td, -1, 0));
          uint32_t old_tw = to_tw(old_topic, w, ntopics);
          CHECK_ERROR_CODE(n_tw.increment_record_oneshot<uint32_t>(context, old_tw, -1, 0));
          CHECK_ERROR_CODE(n_t.increment_record_oneshot<uint32_t>(context, old_topic, -1, 0));
        }

        // Add the word to the new counters
        uint32_t td = to_td(new_topic, d, ntopics);
        CHECK_ERROR_CODE(n_td.increment_record_oneshot<uint32_t>(context, td, 1, 0));
        uint32_t tw = to_tw(new_topic, w, ntopics);
        CHECK_ERROR_CODE(n_tw.increment_record_oneshot<uint32_t>(context, tw, 1, 0));
        CHECK_ERROR_CODE(n_t.increment_record_oneshot<uint32_t>(context, new_topic, 1, 0));

        Epoch ep;
        // because this is not serializable level and the only update is one-shot increment,
        // no race is possible.
        CHECK_ERROR_CODE(xct_manager->precommit_xct(context, &ep));
      } else {
        CHECK_ERROR_CODE(xct_manager->abort_xct(context));
      }
    }

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

void create_count_array(Engine* engine, const char* name, uint64_t size) {
  Epoch ep;
  storage::array::ArrayMetadata meta(name, sizeof(Count), size);
  COERCE_ERROR(engine->get_storage_manager()->create_storage(&meta, &ep));
}

void run_experiment(Engine* engine, const Corpus &corpus) {
  const EngineOptions& options = engine->get_options();

  // first, create empty tables. this is done in single thread
  create_count_array(engine, kNtd, FLAGS_ntopics * corpus.ndocs);
  create_count_array(engine, kNtw, FLAGS_ntopics * corpus.nwords);
  create_count_array(engine, kNt, FLAGS_ntopics);

  LOG(INFO) << "Created arrays";

  // Input information placed in shared memory
  SharedInputs* shared_inputs = reinterpret_cast<SharedInputs*>(
    engine->get_soc_manager()->get_shared_memory_repo()->get_global_user_memory());
  ASSERT_ND(options.soc_.shared_user_memory_size_kb_ * (1ULL << 10)
    >= SharedInputs::get_object_size(corpus.ntokens));
  shared_inputs->init(corpus);

  if (FLAGS_profile) {
    COERCE_ERROR(engine->get_debug()->start_profile("lda.prof"));
  }

  run_workers(engine, shared_inputs, FLAGS_nburnin);
  LOG(INFO) << "Completed burnin!";
  run_workers(engine, shared_inputs, FLAGS_nsamples);
  LOG(INFO) << "Completed final sampling!";

  LOG(INFO) << "Experiment ended.";
  if (FLAGS_profile) {
    engine->get_debug()->stop_profile();
    LOG(INFO) << "Check out the profile result: pprof --pdf lda lda.prof > lda.pdf; okular lda.pdf";
  }

  LOG(INFO) << "Shutting down...";

  LOG(INFO) << engine->get_memory_manager()->dump_free_memory_stat();
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

  {
    Engine engine(options);
    engine.get_proc_manager()->pre_register("worker_task", lda_worker_task);
    COERCE_ERROR(engine.initialize());
    {
      UninitializeGuard guard(&engine);
      run_experiment(&engine, corpus);
      COERCE_ERROR(engine.uninitialize());
    }
  }

  return 0;
}

}  // namespace graphlda
}  // namespace foedus

int main(int argc, char **argv) {
  return foedus::graphlda::driver_main(argc, argv);
}

