{% load kdev_filters %}
/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef {% for ns in namespaces %}{{ ns|upper }}_{% endfor %}{{ name|underscores|upper }}_HPP_
#define {% for ns in namespaces %}{{ ns|upper }}_{% endfor %}{{ name|underscores|upper }}_HPP_

{% for ns in namespaces %}
namespace {{ ns }} {% templatetag openbrace %}
{% endfor %}
/**
 * @brief Brief description of this class.
 * @details
 * Detailed description of this class.
 */
class {{ name }} {
 public:
  /**
   * Description of constructor.
   */
  {{ name }}();

  /**
   * Description of destructor.
   */
  ~{{ name }}();
 protected:
 private:
};

{% for ns in namespaces reversed %}
{% templatetag closebrace %}  // namespace {{ ns }}
{% endfor %}
#endif  // {% for ns in namespaces %}{{ ns|upper }}_{% endfor %}{{ name|underscores|upper }}_HPP_
