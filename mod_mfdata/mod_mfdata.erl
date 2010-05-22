%% @author Tim Benniks <tim@timbenniks.nl>
%% @copyright 2009 Tim Benniks
%% @date 2009-17-08
%%
%% @doc Module implementing the Mediafonds web site

-module(mod_mfdata).
-author("Arjan Scherpenisse <arjan@scherpenisse.net>").
-behaviour(gen_server).

-mod_title("Mediafonds data").
-mod_description("Data model for the Mediafonds website").
-mod_prio(101).

%% gen_server exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/1]).

%% interface functions
-export([
         file_dropped/2,
         do_custom_pivot/2
]).

-include_lib("zotonic.hrl").

-record(state, {context}).

%%====================================================================
%% API
%%====================================================================
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
start_link(Args) when is_list(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore               |
%%                     {stop, Reason}
%% @doc Initiates the server.
init(Args) ->
    process_flag(trap_exit, true),
    {context, Context} = proplists:lookup(context, Args),
    z_notifier:observe(dropbox_file,   {?MODULE, file_dropped}, Context),
    z_notifier:observe(custom_pivot,   {?MODULE, do_custom_pivot}, Context),
    install_check(Context),
    {ok, #state{context=z_context:new(Context)}}.

%% @spec handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%% @doc Trap unknown calls
handle_call(Message, _From, State) ->
    {stop, {unknown_call, Message}, State}.


to_import_dir(Dir, Filename) ->
    filename:join([code:lib_dir(zotonic, priv), "sites",
                   "mediafonds", "files", "import", Dir, filename:basename(Filename)]).


file_dropped({dropbox_file, File}, Context) ->
    handle_import(File, Context).

handle_import(File, Context) ->
    Base = filename:basename(File),
    case mfdata:map_file(Base) of
        undefined ->
			%% The dropbox server will move the file away (after an hour)
			%% TODO: check the dropbox server, should it send an error to the admin?
            %% Vars = [{filename, Base}, {files, mfdata:map_all()}],
            %% import_mail("import_result_mail_invalidfile.tpl", Vars, Context),
			undefined;
        Props ->
            Record = proplists:get_value(def, Props),

            ?DEBUG("FILE DROPPED!"),
            F1 = to_import_dir("processing", File),
            file:rename(File, F1),
            %?DEBUG("recoding..."),
            %os:cmd("recode ms-ansi..u8 " ++ F1),
            F = fun(C) ->
                        try
                            mfimport:import(F1, Record, C)
                        catch
                            _M:E ->
                                Err = {error, E, erlang:get_stacktrace()},
                                ?DEBUG(Err),
                                Err
                        end
                end,

            case z_acl:sudo(F, Context) of
                {error, Error, Stack} ->
                    St = io_lib:format("~p", [Stack]),
                    Er = io_lib:format("~p", [Error]),
                    Vars = [{filename, Base}, {def, Props}, {error, Er}, {stack, St}],
                    import_mail("import_result_mail_fatal.tpl", Vars, Context),
                    To2 = z_email:get_admin_email(Context),
                    z_email:send_render(To2, "import_result_mail_fatal.tpl", Vars, Context);
                Result ->

                    %% E-mail the result to the address configured in  mod_mfdata.mail_to,
                    %% only when notification is done for this type of import.

                    Vars = [{filename, Base}, {result, Result}],
                    import_mail("import_result_mail.tpl", Vars, Context),

                    %% Always send everything to the sysadmin
                    To2 = z_email:get_admin_email(Context),
                    z_email:send_render(To2, "import_result_mail.tpl", Vars, Context)
            end,

            ?DEBUG("Import done!"),
            file:rename(F1, to_import_dir("processed", F1)),
            received
    end.


import_mail(Template, Vars, Context) ->
    case m_config:get_value(?MODULE, mail_to, false, Context) of
        false ->
            nothing;
        A ->
            To = z_convert:to_list(A),
            z_email:send_render(To, Template, Vars, Context)
    end.


%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @doc Trap unknown casts
handle_cast(Message, State) ->
    {stop, {unknown_cast, Message}, State}.



%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% @doc Handling all non call/cast messages
handle_info(_Info, State) ->
    {noreply, State}.

%% @spec terminate(Reason, State) -> void()
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
terminate(_Reason, State) ->
    z_notifier:detach(file_dropped, {?MODULE, file_dropped}, State#state.context),
    z_notifier:detach(custom_pivot, {?MODULE, do_custom_pivot}, State#state.context),
    ok.

%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc Convert process state when code is changed

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%====================================================================
%% support functions
%%====================================================================


do_custom_pivot({custom_pivot, Id}, Context) ->
    CatId = m_rsc:p(Id, category_id, Context),
    case m_category:is_a(CatId, mf_grant, Context) of
        true ->
            %% Check if we have a requestor
            Requestor = case m_rsc:o(Id, has_requestor, Context) of
                            {rsc_list, [Req|_]} ->
                                %% Yes; put the title of the first requestor in the pivot for sorting.
                                z_string:truncate(z_pivot_rsc:get_pivot_title(Req, Context), 80, "");
                            _ ->
                                undefined
                        end,
            Maker = case m_rsc:s(Id, maker_of, Context) of
                        {rsc_list, [Mak|_]} ->
                            z_string:truncate(m_rsc:p(Mak, name_surname, Context), 80, "");
                        _ ->
                            undefined
                    end,
            {?MODULE, [{requestor, Requestor}, {maker, Maker}]};
        _ ->
            none
    end.



%% @doc Check if the mod_mfdata module has been installed.  If not then install all db tables and rscs.
install_check(Context) ->

    F = fun(Ctx) ->
                z_datamodel:manage(?MODULE, datamodel(), Ctx),
                z_pivot_rsc:define_custom_pivot(?MODULE, [{requestor, "varchar(80)"}, {maker, "varchar(80)"}], Ctx)
        end,

    C1 = z_acl:logon(1, Context),
    z_acl:sudo(F, C1).




datamodel() ->

    [
     {categories,
      [

       {mf_data,
        undefined,
        [{title, <<"Mediafonds">>}]},

       {mf_grant, %name
        mf_data, %parent
        [ %props
          {title, <<"Toekenning">>}
         ]},

       {mf_grant_tv,
        mf_grant,
        [{title, <<"Toekenning TV">>}]
        },

       {mf_grant_radio,
        mf_grant,
        [{title, <<"Toekenning Radio">>}]
        },

       {mf_grant_eculture,
        mf_grant,
        [{title, <<"Toekenning E-cultuur">>}]
        },

       {mf_grant_other,
        mf_grant,
        [{title, <<"Toekenning Overig">>}]
        },

       {mf_edition,
        mf_data,
        [{title, <<"Editie">>}]},
       {mf_pp_project,
        mf_data,
        [{title, <<"Stimuleringsbeleidproject">>}]},
       {mf_cluster,
        mf_data,
        [{title, <<"Cluster">>}]},
       {mf_grantcode,
        mf_data,
        [{title, <<"Subsidiecode">>}]},
       {mf_institution,
        mf_data,
        [{title, <<"Instelling">>}]},

       {mf_award,
        keyword,
        [{title, <<"Prijs">>}]},
       {mf_programtype,
        keyword,
        [{title, <<"Genre">>}]},

       {mf_maker,
        person,
        [{title, <<"Maker">>}]},

       {mf_channel,
        mf_data,
        [{title, <<"Kanaal">>}]},

       {mf_broadcast,
        event,
        [{title, <<"Uitzending">>}]},

       {mf_maker_predicate,
        predicate,
        [{title, <<"Maker rol">>}]},

       {mf_publication,
        text,
        [{title, <<"Publicatie">>}]},

       {mf_weekoverview,
        text,
        [{title, <<"Weekoverzicht">>}]},

       {mf_homepage,
        text,
        [{title, <<"Homepage">>}]}
       
      ]
     },

     {predicates,
      [
       {has_cluster,
        [{title, <<"Onderdeel van cluster">>}],
        [{mf_grant, mf_cluster}]},

       {broadcast_on,
        [{title, <<"Op kanaal">>}],
        [{mf_broadcast, mf_channel}]},

       {broadcast_of,
        [{title, <<"Is uitzending van">>}],
        [{mf_broadcast, mf_grant}]},

       {has_programtype,
        [{title, <<"Genre">>}],
        [{mf_grant, mf_programtype}]},

       {has_grantcode,
        [{title, <<"Subsidiecode">>}],
        [{mf_grant, mf_grantcode}]},

       {awarded_to,
        [{title, <<"Uitgereikt aan">>}],
        [{mf_award, mf_grant}]},

       {awarded_by,
        [{title, <<"Uitgereikt door">>}],
        [{mf_award, mf_institution}]},

       {has_external_producer,
        [{title, <<"Extern producent">>}],
        [{mf_grant, mf_institution}]},

       {has_requestor,
        [{title, <<"Aanvrager">>}],
        [{mf_grant, mf_institution}]},

       {maker_of,
        [{title, <<"Maker">>}],
        [{mf_maker, mf_grant}]},

       {has_edition,
        [{title, <<"Editie">>}],
        [{mf_grant, mf_edition}]},

       {has_pp_project,
        [{title, <<"Stimuleringsbeleidproject">>}],
        [{mf_edition, mf_pp_project}]},

       {has_partner,
        [{title, <<"Partner">>}],
        [{mf_pp_project, mf_institution}]},

       {related,
        [{title, <<"Lees meer">>}],
        [{mf_pp_project, text},
         {text, text}]},

       {homepage_left_column,
        [{title, <<"Homepagina 'actueel' linkerkolom">>}],
        [{mf_homepage, text},
         {mf_homepage, event},
         {mf_homepage, mf_data}
        ]},
       
       {homepage_right_column,
        [{title, <<"Homepagina 'actueel' rechterkolom">>}],
        [{mf_homepage, text},
         {mf_homepage, event},
         {mf_homepage, mf_data}
        ]}

      ]
     },

     {resources,
      [
       {page_contact,
        article,
        [{title, <<"Contact">>}, {page_path, <<"/contact">>}]
       },
       {page_fonds,
        article,
        [{title, <<"Het fonds">>}, {page_path, <<"/het-fonds">>}]
       },
       {page_english,
        article,
        [{title, <<"English">>}, {page_path, <<"/english">>}]
       },
       {page_faq,
        article,
        [{title, <<"F.A.Q.">>}, {page_path, <<"/faq">>}]
       },
       {page_links,
        article,
        [{title, <<"Links">>}, {page_path, <<"/links">>}]
       },
       {page_sitemap,
        article,
        [{title, <<"Sitemap">>}, {page_path, <<"/sitemap">>}]
       },
       {page_disclaimer,
        article,
        [{title, <<"Disclaimer">>}, {page_path, <<"/disclaimer">>}]
       },
       {page_colofon,
        article,
        [{title, <<"Colofon">>}, {page_path, <<"/colofon">>}]
       }
      ]
     }
    ].

