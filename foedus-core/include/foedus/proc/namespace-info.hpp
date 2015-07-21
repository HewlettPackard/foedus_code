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
#ifndef FOEDUS_PROC_NAMESPACE_INFO_HPP_
#define FOEDUS_PROC_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::proc
 * @brief System and User \b Procedures.
 * @details
 * This module manages computational tasks (\e Procedures here after) that can be executed on
 * worker threads.
 *
 * @par What is a procedure
 * In a nutshell, a procedure receives inputs and runs on one worker thread, then outputs
 * the results along with error code/stack if failed. This is the unit of tasks one can
 * give to \ref THREAD module.
 * Input/output are of arbitrary format. FOEDUS simply receives/sends a byte array of up to
 * some size (configuration parameter) as data via shared memory.
 * It's left to the procedure to serialize/deserialize the values. The only constraint is that
 * the data must be self-contained. You can't put pointers in it. If some result must be
 * stored in remotely-accessible manner, you can put them in a storage.
 *
 * @par Procedure signature
 * Every procedure has the following function signature.
 * @code{.cpp}
 * ErrorStack procedure(const ProcArguments& args);
 * @endcode
 * \li \b context is the thread context of the pre-allocated thread that runs the impersonated
 * session (see \ref THREADPOOL for more details about impersonation).
 * \li \b input_buffer is an arbitrary input data given by the user themselves as of impersonation.
 * \li \b output_buffer is an arbitrary output data that will be returned to the session
 * when the procedure completes.
 *
 * @par Granulality of a procedure
 * Although a procedure in usual databases run just one transaction,
 * a procedure in FOEDUS can run an arbitary number of transactions.
 * A procedure can be the entire user application, too.
 * Think of our procedure as a way to \e distribute user code to each SOC.
 * Once attached to a worker thread, a procedure can run arbitrary code and it is the intended use.
 * If you attach/detach an individual transaction as one procedure, the inter-process communication
 * will cost a lot.
 *
 * @par System and User Procedures
 * System procedures are a fixed set of procedures provided by FOEDUS itself.
 * Users can invoke them, but they cannot add/modify them. As all system stored procedures are
 * built-in to FOEDUS library, they are automatically avaialble in all SOCs.
 * Names of system stored procedures start with "sys_".
 * User procedures are what user code defines. To register the procedures, the user code has
 * to do one of the followings.
 *
 * @par Local-only User Procedures (function pointer)
 * This is the easiest way to define and execute user procedures. In fact, most test code
 * uses this handy procedure type.
 * The user simply defines a method and registers function pointer.
 * However, this type of procedures can be used only when:
 *  \li child SOC engines are of foedus::EngineType::kChildEmulated type.
 *  \li child SOC engines are of foedus::EngineType::kChildForked type and the user pre-registers
 * the function pointer \b BEFORE the fork (Engine's initialize()).
 *
 * @par Individually registered User Procedures
 * Otherwise, the user has to register the procedures in each SOC because multiple processes
 * do not share address space nor have necessarily same set of shared libraries loaded.
 * We provide a few ways to register procedures in that case.
 *  \li Shared libraries. Users can specify file/folder of shared libraries to load in each SOC.
 * At the start up of each SOC, we load these shared libraries, which can register procedures.
 *  \li Overwrite main() and use spawn() type of SOC launching. User can write their main()
 * so that it initializes child SOC engines and then registers function pointers locally.
 *
 * @see SOC
 * @see foedus::EngineType
 */

/**
 * @defgroup PROC System and User Procedures
 * @ingroup COMPONENTS
 * @copydoc foedus::proc
 */

#endif  // FOEDUS_PROC_NAMESPACE_INFO_HPP_
