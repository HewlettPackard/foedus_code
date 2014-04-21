/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine_options.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/fs/path.hpp>
#include <tinyxml2.h>
#include <string>
#include <sstream>
namespace foedus {
EngineOptions::EngineOptions() {
}
EngineOptions::EngineOptions(const EngineOptions& other) {
    operator=(other);
}

EngineOptions& EngineOptions::operator=(const EngineOptions& other) {
    cache_ = other.cache_;
    debugging_ = other.debugging_;
    log_ = other.log_;
    memory_ = other.memory_;
    savepoint_ = other.savepoint_;
    snapshot_ = other.snapshot_;
    storage_ = other.storage_;
    thread_ = other.thread_;
    xct_ = other.xct_;
    return *this;
}

ErrorStack EngineOptions::load(tinyxml2::XMLElement* element) {
    CHECK_ERROR(get_child_element(element, "CacheOptions", &cache_));
    CHECK_ERROR(get_child_element(element, "DebuggingOptions", &debugging_));
    CHECK_ERROR(get_child_element(element, "LogOptions", &log_));
    CHECK_ERROR(get_child_element(element, "MemoryOptions", &memory_));
    CHECK_ERROR(get_child_element(element, "SavepointOptions", &savepoint_));
    CHECK_ERROR(get_child_element(element, "SnapshotOptions", &snapshot_));
    CHECK_ERROR(get_child_element(element, "StorageOptions", &storage_));
    CHECK_ERROR(get_child_element(element, "ThreadOptions", &thread_));
    CHECK_ERROR(get_child_element(element, "XctOptions", &xct_));
    return RET_OK;
}

ErrorStack EngineOptions::save(tinyxml2::XMLElement* element) const {
    CHECK_ERROR(insert_comment(element, "Set of options given to the engine at start-up"));
    CHECK_ERROR(add_child_element(element, "CacheOptions", "", cache_));
    CHECK_ERROR(add_child_element(element, "DebuggingOptions", "", debugging_));
    CHECK_ERROR(add_child_element(element, "LogOptions", "", log_));
    CHECK_ERROR(add_child_element(element, "MemoryOptions", "", memory_));
    CHECK_ERROR(add_child_element(element, "SavepointOptions", "", savepoint_));
    CHECK_ERROR(add_child_element(element, "SnapshotOptions", "", snapshot_));
    CHECK_ERROR(add_child_element(element, "StorageOptions", "", storage_));
    CHECK_ERROR(add_child_element(element, "ThreadOptions", "", thread_));
    CHECK_ERROR(add_child_element(element, "XctOptions", "", xct_));
    return RET_OK;
}

ErrorStack EngineOptions::load_from_file(const fs::Path& path) {
    tinyxml2::XMLDocument document;
    if (!fs::exists(path)) {
        return ERROR_STACK_MSG(ERROR_CODE_CONF_FILE_NOT_FOUNT, path.c_str());
    }

    tinyxml2::XMLError load_error = document.LoadFile(path.c_str());
    if (load_error != tinyxml2::XML_SUCCESS) {
        std::stringstream custom_message;
        custom_message << "problemtic file=" << path << ", tinyxml2 error=" << load_error
             << ", GetErrorStr1()=" << document.GetErrorStr1()
             << ", GetErrorStr2()=" << document.GetErrorStr2();
        return ERROR_STACK_MSG(ERROR_CODE_CONF_PARSE_FAILED, custom_message.str().c_str());
    } else if (!document.RootElement()) {
        return ERROR_STACK_MSG(ERROR_CODE_CONF_EMPTY_XML, path.c_str());
    } else {
        CHECK_ERROR(load(document.RootElement()));
    }
    return RET_OK;
}

ErrorStack EngineOptions::save_to_file(const fs::Path& path) const {
    // construct the XML in memory
    tinyxml2::XMLDocument document;
    tinyxml2::XMLElement* root = document.NewElement("EngineOptions");
    CHECK_OUTOFMEMORY(root);
    document.InsertFirstChild(root);
    CHECK_ERROR(save(root));

    fs::Path folder = path.parent_path();
    // create the folder if not exists
    if (!fs::exists(folder)) {
        if (!fs::create_directories(folder, true)) {
            std::stringstream custom_message;
            custom_message << "file=" << path << ", folder=" << folder << ", errno=" << errno;
            return ERROR_STACK_MSG(ERROR_CODE_CONF_MKDIRS_FAILED, custom_message.str().c_str());
        }
    }

    // To atomically save a file, we write to a temporary file and call sync, then use POSIX rename.
    fs::Path tmp_path(path);
    tmp_path += ".tmp_";
    tmp_path += fs::unique_path("%%%%%%%%");

    tinyxml2::XMLError save_error = document.SaveFile(tmp_path.c_str());
    if (save_error != tinyxml2::XML_SUCCESS) {
        std::stringstream custom_message;
        custom_message << "problemtic file=" << path << ", tinyxml2 error=" << save_error
             << ", GetErrorStr1()=" << document.GetErrorStr1()
             << ", GetErrorStr2()=" << document.GetErrorStr2();
        return ERROR_STACK_MSG(ERROR_CODE_CONF_COULD_NOT_WRITE, custom_message.str().c_str());
    }

    if (!fs::durable_atomic_rename(tmp_path, path)) {
        std::stringstream custom_message;
        custom_message << "dest file=" << path << ", src file=" << tmp_path
            << ", errno=" << errno;
        return ERROR_STACK_MSG(ERROR_CODE_CONF_COULD_NOT_RENAME, custom_message.str().c_str());
    }

    return RET_OK;
}


}  // namespace foedus
