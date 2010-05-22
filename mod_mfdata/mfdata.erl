%% @author Arjan Scherpenisse <arjan@scherpenisse.net>
%%
%% Data import for mediafonds.  This file contains the definitions:
%% how to transform the input records (defined in mfrecords.erl) into
%% resources.

-module(mfdata).

-include_lib("zotonic.hrl").

-export([import_definition/1, send_notification/1, map_all/0, map_file/1]).



map_all() ->
    [ 
      [{file, "aanvragen.txt"},
       {def, import_mf_grant},
       {title, "Aanvragen (toekenningen)"}],
      [{file, "clusters.txt"},
       {def, import_mf_cluster},
       {title, "Clusters"}],
      [{file, "edities.txt"},
       {def, import_mf_edition},
       {title, "Editites"}],
      [{file, "makers.txt"},
       {def, import_mf_maker},
       {title, "Makers"}],
      [{file, "prijzen.txt"},
       {def, import_mf_award},
       {title, "Prijzen"}],
      [{file, "stimuleringsbeleid.txt"},
       {def, import_mf_pp_project},
       {title, "Stimuleringsbeleid-projecten"}],
      [{file, "uitzendingen.txt"},
       {def, import_mf_broadcast},
       {title, "Uitzendingen"}]
     ].

map_file(Filename) ->
    F = fun(R) ->
                case proplists:get_value(file, R) of
                    Filename ->
                        true;
                    _ -> 
                        false
                end
        end,
    L = lists:filter(F, map_all()),
    case L of
        [] ->
            undefined;
        [Record] ->
            Record
    end.


%% Send notification? Configured per import file.
send_notification("mailing_week.txt") -> true;
send_notification("mailing.txt")      -> true;
send_notification(_)                  -> false.


grantcode_to_grant_category(Cat) ->
    case Cat of

        <<"gamefonds">> ->
            <<"mf_grant_eculture">>;
        <<"ontw.ecult.">> ->
            <<"mf_grant_eculture">>;
        <<"ontw.e-cult.">> ->
            <<"mf_grant_eculture">>;
        <<"e-cultuur">> ->
            <<"mf_grant_eculture">>;

        <<"ontw.radio">> ->
            <<"mf_grant_radio">>;
        <<"radio">> ->
            <<"mf_grant_radio">>;

        <<"ontw.tv">> ->
            <<"mf_grant_tv">>;
        <<"tv">> ->
            <<"mf_grant_tv">>;

        <<"varia">> ->
            <<"mf_grant_other">>;
        <<>> ->
            <<"mf_grant_other">>;

        V ->
            ?DEBUG("INVALID GRANT CODE!!"),
            ?DEBUG(V),
            <<"mf_grant_other">>
                end.


%%
%% The definition for mapping the import records to resources.
%%
import_definition(import_mf_pp_project) ->
    [ {
       %% Field mapping
       [
        {category, <<"mf_pp_project">>},
        {name, code},
        {title, {html_escape, title}},
        {body, {concat, [<<"<p>">>, {html_escape, description}, <<"</p>">>]}}
        ],
       []
      }
     ];

import_definition(import_mf_edition) ->
    [  {
       %% Field mapping
       [
        {category, <<"mf_edition">>},
        {name, code},
        {title, {html_escape, title}},
        {body, {concat, [<<"<p>">>, {html_escape, description}, <<"</p>">>]}}
        ],
       %% Edges
       [
        {has_pp_project, {<<"mf_pp_project">>, pp_project_code}}
       ]
      }
     ];

import_definition(import_mf_cluster) ->
    [ {
       %% Field mapping
       [
        {category, <<"mf_cluster">>},
        {name, code},
        {title, {html_escape, {prefer, [title, code]}}}
       ],
       %% Edges
       []
      }
     ];


import_definition(import_mf_grant) ->
    [      
      { 
       %% field mapping
       [
        {category, <<"mf_grantcode">>},
        {name, grant_code},
        {title, {html_escape, grant_code}}
       ],
       %% edges
       []
      },
     
     {
       %% field mapping
       [
        {category, <<"mf_programtype">>},
        {name, category_code},
        {title, {html_escape, category_code}}
       ],
       %% edges
       []
      },

     
     {
       %% Field mapping
       [
        {category, <<"mf_institution">>},
        {name, external_producer},
        {title, {html_escape, external_producer}},
        {import_skip_delete, true}
       ],
       [
       ]},

      {
       %% Field mapping
       [
        {category, <<"mf_institution">>},
        {name, requestor},
        {title, {html_escape, requestor}},
        {import_skip_delete, true}
       ],
       [
       ]},

           {
       %% Field mapping
       [
        %% Regular rsc records
        {category, fun(Row) -> grantcode_to_grant_category(proplists:get_value(grant_code, Row)) end},
        {name, {name_prefix, <<"mf_grant">>, file_nr}},
        {title, {html_escape, {prefer, [title_broadcast, title_working]}}},
        {summary,  {html_escape, {prefer, [description_broadcast, description]}}},

        {date_start, {datetime, last_date, <<"00:00:00">>}},
        {date_end, {datetime, last_date, <<"00:00:59">>}},

        %% Extra information for a grant
        {mf_grantcode, file_nr},
        {mf_last_date, {date, last_date}},

        {website, url}
       ],
       
       %% Outgoing edges
       [
        {has_programtype, {<<"mf_programtype">>, category_code}},
        {has_grantcode, {<<"mf_grantcode">>, grant_code}},
        {has_cluster, {<<"mf_cluster">>, cluster_code}},
        {has_edition, {<<"mf_edition">>, edition_code}},
        {has_external_producer, {<<"mf_institution">>, external_producer}},
        {has_requestor, {<<"mf_institution">>, requestor}}
       ]
      }
          ];

import_definition(import_mf_maker) ->
    [ {
       %% Regular rsc records
       [
        {category, <<"mf_maker_predicate">>},
        {name, function},
        {title, {html_escape, function}},

        {callbacks, [{rsc_insert, fun(Id, _Props, C) ->
                                          ObjSubj = [
                                               {Id, true,  m_category:name_to_id_check('mf_maker', C)},
                                               {Id, false, m_category:name_to_id_check('mf_grant', C)}
                                              ],
                                          [ 1 = z_db:q("insert into predicate_category (predicate_id, is_subject, category_id) values ($1, $2, $3)", OS, C) || OS <- ObjSubj ]
                                  end
                     }]}
       ],
       []
      },

      {
       [
        %% Regular rsc records                 
        {category, <<"mf_maker">>},
        {name, unique_id},
        {title, {html_escape, {concat, [name_first, {surroundspace, prefix}, name_last]}}},

        %% Extra information for a maker
        {name_first, name_first},
        {name_surname,  name_last},
        {name_surname_prefix, prefix}
       ],
      %% Edges
      [
        {maker_of, {<<"mf_grant">>, file_nr}},
        {{<<"mf_maker_predicate">>, function}, {<<"mf_grant">>, file_nr}}
      ]}
     ];

import_definition(import_mf_award) ->
    [ {
       [
        %% Regular rsc records                 
        {category, <<"mf_award">>},
        {name, {concat, [file_nr, <<" ">>, organisation, <<" ">>, title]}},
        {summary, {html_escape, organisation}},
        {title, {html_escape, title}},
        {date_start, {datetime, date, <<"00:00:00">>}}
       ],
       %% Edges
       %% put on edge: {prize_date}
       [
        {awarded_to, {<<"mf_grant">>, file_nr}},
        {awarded_by, {<<"mf_institution">>, organisation}}
       ]},

      {
       [
        %% Regular rsc records                 
        {category, <<"mf_institution">>},
        {name, organisation},
        {title, {html_escape, organisation}},
        {import_skip_delete, true}
       ],
       %% Edges
       [
       ]}
     ];

import_definition(import_mf_broadcast) ->
    [ {
       %% fields
       [
        {category, <<"mf_broadcast">>},
        {name, {concat, [file_nr, <<" ">>, channel, <<" ">>, broadcast_date, <<" ">>, time_start]}},
        {title, {html_escape, {prefer, [title]}}},
        {summary, {html_escape, description}},  %% FIXME -- if not set, get this data from the linked mf_grant!
        {is_featured, true},

        {date_start, {datetime, broadcast_date, time_start}},
        {date_end, {datetime, broadcast_date, time_end}},

        {mf_broadcast_date, {date, broadcast_date}},
        {mf_time_start, {time, time_start}},
        {mf_time_end, {time, time_end}}
       ],
       % Edges
       [
        {broadcast_of, {<<"mf_grant">>, file_nr}},
        {broadcast_on, {<<"mf_channel">>, channel}}
       ]
      },
      
      {
       [
        {category, <<"mf_channel">>},
        {name, channel},
        {title, {html_escape, channel}}
       ],
        [
        ]}
     ].

