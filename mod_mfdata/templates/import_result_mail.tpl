<html>
    <head>
        <title>{% block subject %}Import result: {{ filename }}{% endblock %}</title>
    </head>
    <body>
        {% block content %}
        <h2>Import result</h2>

        <p>
            Processed filename: <tt>{{ filename }}</tt>
        </p>

        <h3>Processed entries</h3>
        <ul>
            {% for cat, v in result.seen %}
            <li><b>{{ m.rsc[cat].title|default:cat }}</b> &mdash; {{ v }} records.</li>
            {% empty %}
            <li>No records.</li>
            {% endfor %}
        </ul>

        <h3>New entries</h3>
        <ul>
            {% for cat, v in result.new %}
            <li><b>{{ m.rsc[cat].title|default:cat }}</b> &mdash; {{ v }} records.</li>
            {% empty %}
            <li>No records.</li>
            {% endfor %}
        </ul>

        <h3>Updated entries</h3>
        <ul>
            {% for cat, v in result.updated %}
            <li><b>{{ m.rsc[cat].title|default:cat }}</b> &mdash; {{ v }} records.</li>
            {% empty %}
            <li>No records.</li>
            {% endfor %}
        </ul>

        <h3>Deleted entries: {{ result.deleted }}</h3>

        {% endblock %}
    </body>
</html>

