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
#ifndef FOEDUS_ATTACHABLE_HPP_
#define FOEDUS_ATTACHABLE_HPP_

#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
namespace foedus {
/**
 * @brief Attachable Resources on Shared Memory.
 * @ingroup IDIOMS
 * @details
 * @par Objects that live in Shared Memory
 * FOEDUS has several objects whose data are stored in shared memory so that all SOCs
 * can access them. Such objects have a certain pattern for being initialized and used. Namely:
 *  \li The master engine allocates shared memory where the entire data or at least shared part of
 * the object are placed.
 *  \li Either the master engine or some SOC \e initializes the shared data on shared memory.
 *  \li Other SOCs \e attach the shared data to a local object.
 *  \li When FOEDUS shuts down, either the master engine or some SOC \e uninitializes the shared
 * data.
 *
 * @par Control block, or a pointer to Shared Memory
 * The essense of these attachable objects is the \e control-block, the shared data of shared
 * memory. Typically, the only data element of an attachable object is the pointer to
 * shared memory. An attachable object can have non-shared data, and in fact many attachable
 * objects do so, but remember that it will make copying more expensive.
 *
 * @par Copyable/Moveable
 * Attachable objects should be trivally copy-able and move-able without any expensive operation
 * because all shared data are on the shared memory. We just copy the pointer to it.
 * This makes several things simpler and more efficient. Rather than we instantiate attachable
 * objects on heap, we often just return/copy them on stack.
 * This is not a mandatory requirement to be attachable, but attachable objects that require
 * frequent instantiate/copy should place almost all data in shared memory.
 *
 * @par Destruct/Detach
 * There is no destruct/detach interface for these objects.
 * To be trivially copy-able, these objects shouldn't need an explicit destruction or detachment.
 * If it's really needed, override the destructor.
 */
template <typename CONTROL_BLOCK>
class Attachable {
 public:
  Attachable() : engine_(CXX11_NULLPTR), control_block_(CXX11_NULLPTR) {}
  explicit Attachable(Engine* engine) : engine_(engine), control_block_(CXX11_NULLPTR) {}
  Attachable(Engine* engine, CONTROL_BLOCK* control_block)
    : engine_(engine), control_block_(CXX11_NULLPTR) {
    attach(control_block);
  }
  explicit Attachable(CONTROL_BLOCK* control_block)
    : engine_(CXX11_NULLPTR), control_block_(CXX11_NULLPTR) {
    attach(control_block);
  }
  virtual ~Attachable() {}

  // copy constructors
  Attachable(const Attachable& other)
    : engine_(other.engine_), control_block_(other.control_block_) {}
  Attachable& operator=(const Attachable& other) {
    engine_ = other.engine_;
    control_block_ = other.control_block_;
    return *this;
  }

  /**
   * @brief Attaches to the given shared memory.
   * @param[in] control_block pointer to shared data on shared memory
   * @pre someone has (or at least will before this object actually does something)
   * initialized this object on the shared memory.
   * @details
   * This method should never fail so that we can provide a trivially copy-able semantics.
   * In many cases, this method should be just setting control_block_ as done in the
   * default implementation below. If the object needs to set more things, override this.
   */
  virtual void attach(CONTROL_BLOCK* control_block) {
    control_block_ = control_block;
  }

  /** Returns whether the object has been already attached to some shared memory. */
  bool            is_attached() const       { return control_block_; }
  CONTROL_BLOCK*  get_control_block() const { return control_block_; }

  Engine* get_engine() const          { return engine_; }
  void    set_engine(Engine* engine)  { engine_ = engine; }

 protected:
  /**
   * Most attachable object stores an engine pointer (local engine), so we define it here.
   * If the object doesn't need it, it can leave this null.
   */
  Engine*         engine_;
  /**
   * The shared data on shared memory that has been initialized in some SOC or master engine.
   */
  CONTROL_BLOCK*  control_block_;
};


}  // namespace foedus
#endif  // FOEDUS_ATTACHABLE_HPP_
