/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_THREAD_THREAD_OPTIONS_HPP_
#define FOEDUS_THREAD_THREAD_OPTIONS_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/externalize/externalizable.hpp>
#include <foedus/thread/thread_id.hpp>
#include <iosfwd>
namespace foedus {
namespace thread {
/**
 * @brief Set of options about threads and thread-groups.
 * @ingroup THREAD
 * @details
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct ThreadOptions CXX11_FINAL : public virtual externalize::Externalizable {
    /**
     * Constructs option values with default values.
     */
    ThreadOptions();

    /**
     * Number of ThreadGroup in the engine.
     * Default value is hardware NUMA node count (::numa_num_configured_nodes()).
     */
    ThreadGroupId         group_count_;

    /**
     * Number of Thread in each ThreadGroup.
     * Default value is hardware NUMA core count;
     * ::numa_num_configured_cpus() / ::numa_num_configured_nodes().
     */
    ThreadLocalOrdinal    thread_count_per_group_;

    ErrorStack load(tinyxml2::XMLElement* element) CXX11_OVERRIDE;
    ErrorStack save(tinyxml2::XMLElement* element) const CXX11_OVERRIDE;
    friend std::ostream& operator<<(std::ostream& o, const ThreadOptions& v) {
        v.save_to_stream(&o);
        return o;
    }
};
}  // namespace thread
}  // namespace foedus
#endif  // FOEDUS_THREAD_THREAD_OPTIONS_HPP_
