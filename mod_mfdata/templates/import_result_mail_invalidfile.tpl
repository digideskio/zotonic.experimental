{% extends "import_result_mail.tpl" %}

{% block subject %}Import error: {{ filename }}{% endblock %}

{% block content %}

<h1>Oops...</h1>

<p>The file you uploaded, <strong>{{ filename }}</strong> is not one
    of the files that can be processed, because it has an invalid filename.</p>

<p>You can upload the following files, which will be imported into the following records:</p>

<ul>
{% for r in files %}
    <li><tt>{{ r.file }}</tt> &mdash; {{ r.title }}</li>
{% endfor %}
</ul>

<p>Please note that the import files need to be encoded using the <em>windows-1252</em> codepage with the <em>ascii character 13 (carriage return)</em> as line separator, and <em>ascii character 9 (tab)</em> as field separator.</p>

{% endblock %}
