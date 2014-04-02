{% load kdev_filters %}
/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <{% for ns in namespaces %}{{ ns }}/{% endfor %}{{ output_file_header }}>

{% for ns in namespaces %}
namespace {{ ns }} {% templatetag openbrace %}
{% endfor %}

{{ name }}::{{ name }}() {
}

~{{ name }}::{{ name }}() {
}

{% for ns in namespaces %}
{% templatetag closebrace %}  // {{ ns }}
{% endfor %}
