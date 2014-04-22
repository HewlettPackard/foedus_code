/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SAVEPOINT_SAVEPOINT_HPP_
#define FOEDUS_SAVEPOINT_SAVEPOINT_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/externalize/externalizable.hpp>
#include <foedus/fs/fwd.hpp>
namespace foedus {
namespace savepoint {
/**
 * @brief The information we maintain in savepoint manager and externalize to a file.
 * @ingroup SAVEPOINT
 * @details
 * This object is a very compact representation of \e progresses of the entire engine.
 * This object holds only a few integers per module to denote upto what we are \e surely done.
 * We update this object for each epoch-based group commit and write out this as an xml
 * durably and atomically.
 */
struct Savepoint CXX11_FINAL : public virtual externalize::Externalizable {
    /**
     * Constructs an empty savepoint.
     */
    Savepoint();

    EXTERNALIZABLE(Savepoint);
};
}  // namespace savepoint
}  // namespace foedus
#endif  // FOEDUS_SAVEPOINT_SAVEPOINT_HPP_
