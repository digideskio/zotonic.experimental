{% extends "admin_base.tpl" %}

{% block title %}Mediafonds module{% endblock %}

{% block content %}

	<div id="content" class="zp-85">
		<div class="block clearfix">
            
            <h2>Mediafonds</h2>
            
            <div>
                <p>fsdjklfj sdlfsdjl fasdjf</p>
            </div>

            <div class="clearfix">
                {% all include "_admin_make_page_buttons.tpl" %}
                {% button class="" text="Make a new page" action={dialog_new_rsc title=""} %}
                {% button class="" text="Make a new media item" action={dialog_media_upload title=""} %}
            </div>
            
            <hr />
            
        </div>
    </div>

{% endblock %}
