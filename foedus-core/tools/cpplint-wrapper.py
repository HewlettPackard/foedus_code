#!/usr/bin/python
## Copyright (c) 2014, Hewlett-Packard Development Company, LP.
## The license and distribution terms for this file are placed in LICENSE.txt.
# Wraps Google's cpplint.py for a few additional features;
# 1) Recursively look for files to check. cpplint.py might do this out of box in future.
# 2) Control whether we return a non-zero exit code if there are style errros (--style-error arg).
# 3) Skip checking a file that had no warnings previously and has not changed since then.
import getopt
import os
import json
import re
import string
import subprocess
import sys
import time
import tempfile
import unicodedata


wrapper_usage = """
Syntax: cpplint-wrapper.py
        [--style-error]
        [--excludes=<excluded folder name regex>]
        [--extensions=<file extension regex>]
        [--cpplint-file=<path of cpplint.py>]
        [--history-file=<path of history file that describes the result of previous run>]
        [other flags for cpplint, which are directly passed to cpplint]
        <file/folder> [path] ...

  Flags specific to cpplint-wrapper:

    style-error
      If specified, this python script returns a non-zero exit code if there are some
      style warnings (which is the cpplint's behavior). Default is false.

    excludes
      If specified, files/folders whose names meet this regex are skipped.
      Default is "(\.git|\.svn)".

    extensions
      If specified, files whose extensions meet this regex are verified by cpplint.
      Default is "\.(c|h|cc|cpp|hpp)$".

    cpplint-file
      If specified, the path of cpplint.py. Default is "cpplint.py" (in working folder).

    history-file
      If specified, the path of history file to skip files that had no changes.
      Default is ".lint_history" (in working folder)

    file/folder
      Required argument. The file(s) or folder(s) to recursively look for target files to
      run cpplint on.
"""

style_error = False
excludes = '(\.git|\.svn)'
excludes_regex = re.compile(excludes);
extensions = '\.(c|h|cc|cpp|hpp)$'
extensions_regex = re.compile(extensions);
cpplint_file = 'cpplint.py'
history_file = '.lint_history'

def print_usage(wrong_parameter):
    sys.stderr.write(wrapper_usage)
    if wrong_parameter:
        sys.exit('\Wrong Parameter: ' + wrong_parameter)
    else:
        sys.exit(1)

def parse_arguments(args):
    (opts, names) = getopt.getopt(args, '', ['help',
                                             'style-error', 'excludes=', 'extensions=',
                                             'cpplint-file=', 'history-file=',
                                             # followings are for cpplint
                                             'output=', 'verbose=', 'counting=', 'filter=',
                                             'root=', 'linelength='])
    cpplint_arguments = []
    for (opt, val) in opts:
        if opt == '--help':
            print_usage(None)
        elif opt == '--style-error':
            global style_error
            style_error = True
        elif opt == '--excludes':
            global excludes
            global excludes_regex
            excludes = val
            excludes_regex = re.compile(excludes);
        elif opt == '--extensions':
            global extensions
            global extensions_regex
            extensions = val
            extensions_regex = re.compile(extensions);
        elif opt == '--cpplint-file':
            global cpplint_file
            cpplint_file = val
        elif opt == '--history-file':
            global history_file
            history_file = val
        else:
            cpplint_arguments.append(opt + '=' + val)

    if not names:
        print_usage('No files/folders were specified.')

    return (names, cpplint_arguments)

def get_files_recursive(folder, out_files):
    for name in os.listdir(folder):
        if excludes_regex.search(name) is None:
            path = os.path.join(folder, name)
            if os.path.isfile(path):
                if extensions_regex.search(name) is not None:
                    out_files.append(path)
            else:
                get_files_recursive(path, out_files)

def compute_dir_index(files):
    """ Return a tuple containing a dictionary: filepath => last
    """
    index = {}
    start = time.time()
    for f in files:
        index[f] = str(os.path.getmtime(f))
    end = time.time()
    sys.stdout.write('cpplint-wrapper: Checked timestamp of ' + str(len(files)) + ' files in '
                      + str(end - start) + ' sec.\n')

    return index

def store_dir_index(index, path):
    start = time.time()
    f = open(path, 'w')
    json.dump(index, f)
    f.close()
    end = time.time()
    sys.stdout.write('cpplint-wrapper: Wrote ' + path + ' (' + str(len(index)) + ' entries) in ' + str(end - start) + ' sec.\n')

def load_dir_index(path):
    start = time.time()
    f = open(path, 'r')
    idx = json.load(f)
    f.close()
    end = time.time()
    sys.stdout.write('cpplint-wrapper: Read ' + path + ' (' + str(len(idx)) + ' entries) in ' + str(end - start) + ' sec.\n')
    return idx

def compute_diff(files, index_base, index_now):
    new_index = {}

    for f in files:
        if not f in index_base or index_base[f] != index_now[f]:
            new_index[f] = index_now[f]
    return new_index

def exec_cpplint(files, cpplint_arguments):
    if not files:
        sys.stdout.write('No files to check\n')
        return False

    # run cpplint only for files that have been changed or had some warning previously
    index_last = {}
    if os.path.isfile(history_file):
        index_last = load_dir_index(history_file)
    index_now = compute_dir_index(files)
    index_now = compute_diff(files, index_last, index_now)
    if not index_now:
        sys.stdout.write('cpplint-wrapper: No files have been changed\n')
        return False

    args = [sys.executable, cpplint_file]
    args.append('--extensions=c,cc,cpp,h,hpp,cu,cuh') # cpplint's default extensions lack "hpp"
    args += cpplint_arguments
    for f in index_now:
        args.append(f)

    sys.stdout.write('Launching cpplint for ' + str(len(index_now)) + ' files.\n')
    # sys.stdout.write('arguments: ' + ' '.join(args) + '\n')
    # sys.stdout.write('Launching cpplint (' + cpplint_file + ') for ' + str(len(files))
    #                 + ' files. arguments: ' + ' '.join(args) + '\n')

    tmpfile = tempfile.TemporaryFile()
    proc = subprocess.Popen(args, bufsize=65536, stderr=tmpfile, close_fds=True)
    proc.wait()
    # go to first line of tmpfile
    tmpfile.seek(0)
    has_error = False
    clean_index = {}
    clean_index.update(index_last)
    clean_index.update(index_now)
    for line in tmpfile:
        # This is annoying. Why cpplint writes this to _stderr_??
        if not line.startswith("Done processing "):
            has_error = True
## eg.
## /home/kimurhid/projects/foedus_code/foedus-core/include/foedus/cache/cache_hashtable.hpp:205:  Namespace should be terminated with "// namespace cache"  [readability/namespace] [5]
## We parse the line and remember the file to be excluded from the "clean" list
            if line.find(':') > 0:
              f = line[:line.find(':')]
              if f in clean_index:
                del clean_index[f]
            if sys.stderr.isatty():
              sys.stderr.write('\033[93m' + line + '\033[0m') # Put a color that stands out
            else:
              sys.stderr.write(line) # If the client (kdevelop?) doesn't support, avoid it.

    # store the clean list to speed up next execution
    store_dir_index(clean_index, history_file)

    return has_error

def main():
    (names, cpplint_arguments) = parse_arguments(sys.argv[1:])
    files = []
    for name in names:
        if os.path.isfile(name):
            files.append(name)
        else:
            get_files_recursive(name, files)

    has_error = exec_cpplint(files, cpplint_arguments)
    if has_error and style_error:
        sys.stderr.write('There was cpplint error(s) and --style-error was specified. non-zero exit code.\n')
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == '__main__':
    main()
