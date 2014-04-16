/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_MANAGER_PIMPL_HPP_
#define FOEDUS_XCT_XCT_MANAGER_PIMPL_HPP_
#include <foedus/fwd.hpp>
#include <foedus/initializable.hpp>
#include <foedus/thread/fwd.hpp>
#include <foedus/xct/fwd.hpp>
namespace foedus {
namespace xct {
/**
 * @brief Pimpl object of XctManager.
 * @ingroup XCT
 * @details
 * A private pimpl object for XctManager.
 * Do not include this header from a client program unless you know what you are doing.
 */
class XctManagerPimpl final : public DefaultInitializable {
 public:
    XctManagerPimpl() = delete;
    explicit XctManagerPimpl(Engine* engine) : engine_(engine) {}
    ErrorStack  initialize_once() override;
    ErrorStack  uninitialize_once() override;

    ErrorStack  begin_xct(thread::Thread* context);
    ErrorStack  commit_xct(thread::Thread* context);
    ErrorStack  abort_xct(thread::Thread* context);

    Engine* const           engine_;
};
}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_MANAGER_PIMPL_HPP_
