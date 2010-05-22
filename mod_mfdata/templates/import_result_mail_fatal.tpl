{% extends "import_result_mail.tpl" %}

{% block subject %}Fatal error: {{ filename }}{% endblock %}

{% block content %}

<h1>Fatal error!.</h1>

<p>The file you uploaded, <strong>{{ filename }}</strong> is not in
the right import definition.</p>

<p>Please check the database definition for <strong>{{ def.title
}}</strong> for the column order and numbers.</p>


<p>The error was:</p>
<pre>
{{ error }}
</pre>

<p>If this reads <tt>invalid_nr_of_columns</tt>, then your import file
is not in the right format. In any other case, please contact the
Zotonic team.</p>


<!--
{{ stack }}
-->

{% endblock %}
