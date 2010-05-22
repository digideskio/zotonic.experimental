%% @author Arjan Scherpenisse <arjan@scherpenisse.net>
%% @copyright 2009 Arjan Scherpenisse
%% @date 2009-10-29
%%
%% Mediafonds importer

-module(mfimport).

-export([import/3, rstrip/1, lstrip/1, strip/1, clear_category/2, set_category_managed/3, managed_edge_lookup/2]).

-include_lib("zotonic.hrl").

-record(importresult, {seen=[], new=[], updated=[], errors=[], ignored=[], deleted=0}).
-record(state, {context, name_to_id, managed_edges, to_flush=[], result, managed_resources}).

increase(Key, Record) ->
    case proplists:get_value(Key, Record) of
        undefined ->
            [{Key, 1} | Record];
        N -> [{Key, N+1} | proplists:delete(Key, Record)]
    end.

add_result_ignored(Type, S=#state{result=R}) ->
    S#state{result=R#importresult{ignored=increase(Type, R#importresult.ignored)}}.

add_result_seen(Type, S=#state{result=R}) ->
    S#state{result=R#importresult{seen=increase(Type, R#importresult.seen)}}.

add_result_new(Type, S=#state{result=R}) ->
    S#state{result=R#importresult{new=increase(Type, R#importresult.new)}}.

add_result_updated(Type, S=#state{result=R}) ->
    S#state{result=R#importresult{updated=increase(Type, R#importresult.updated)}}.

add_result_error(_Type, Error, S=#state{result=R}) ->
    S#state{result=R#importresult{errors=[Error|R#importresult.errors]}}.



import(File, Record, Context) ->
    Rows = read_import_file(File, mfrecords:Record()),

    State0 = #state{
      context=Context, 
      result=#importresult{}, 
      managed_resources=sets:new()
     },

    State1 = reload_name_lookup(State0),
    State = reload_managed_edge_lookup(State1),
    
    NewState = import_rows(Rows, mfdata:import_definition(Record), State),

    % Flush the changed resources
    [z_depcache:flush(Id, Context) || Id <- NewState#state.to_flush],


    %% Comparte the managed resources; deleting all resources from the db that were managed before but now unmanaged.
    DbManaged = case m_config:get(import_managed_resources, Record, Context) of
                    undefined ->
                        sets:new();
                    Props ->
                        proplists:get_value(set, Props)
                end,
    ?DEBUG("managed:"),
    ?DEBUG(DbManaged),

    DeleteIds = sets:to_list(sets:subtract(DbManaged, NewState#state.managed_resources)),

    Del = fun(Id) ->
                  case m_rsc:exists(Id, Context) of
                      true ->
                          m_rsc:update(Id, [{is_protected, false}], Context),
                          m_rsc:delete(Id, Context);
                      false ->
                          ok
                  end
          end,

    [ok=Del(Id) || Id <- DeleteIds],
    ?DEBUG("deleted:"),
    ?DEBUG(DeleteIds),

    m_config:set_prop(import_managed_resources, Record, set, NewState#state.managed_resources, Context),

    % FIXME: compare managed edges.

    %% Return the result of the import.
    R = NewState#state.result,
    [ {seen, R#importresult.seen},
      {new,  R#importresult.new},
      {updated, R#importresult.updated},
      {errors, R#importresult.errors},
      {deleted, length(DeleteIds)},
      {ignored, R#importresult.ignored}].

import_rows(Rows, {Def, Opts}, State) ->

    State1 = case proplists:get_value(on_init, Opts) of
                 undefined -> State;
                 CB -> CB(State#state.context),
                       reload_name_lookup(State)
             end,
    import_rows1(Rows, Def, State1);

import_rows(Rows, Def, State) ->
    import_rows1(Rows, Def, State).

import_rows1([], _Def, State) ->
    State;
import_rows1([Row|Rest], Def, State) ->
    %F = fun(_Ctx) ->
    State1 = import_parts(Row, Def, State),
    %    end,
    %State1 = z_db:transaction(F, State#state.context),
    import_rows1(Rest, Def, State1).



import_parts(_Row, [], State) ->
    State;
import_parts(Row, [Def | Definitions], State) ->
    {FieldMapping, ConnectionMapping} = Def,
    case import_def_rsc(FieldMapping, Row, State) of
        {S, {ignore}} ->
            import_parts(Row, Definitions, S);
        {S, {error, Type, E}} ->
            ?DEBUG(E),
            add_result_seen(Type, add_result_error(Type, E, S));

        {S, {new, Type, Id, Name}} ->
            State0 = add_managed_resource(Id, FieldMapping, S),
            State1 = add_name_lookup(State0, Name, Id),
            State2 = import_parts(Row, Definitions, State1),
            import_def_edges(Id, ConnectionMapping, Row, State2),
            add_result_seen(Type, add_result_new(Type, State2));

        {S, {equal, Type, Id}} ->
            State0 = add_managed_resource(Id, FieldMapping, S),
            State1 = import_parts(Row, Definitions, State0),
            import_def_edges(Id, ConnectionMapping, Row, State1),
            add_result_seen(Type, add_result_ignored(Type, State1));

        {S, {updated, Type, Id}} ->
            %% Equal
            State0 = add_managed_resource(Id, FieldMapping, S),
            State1 = import_parts(Row, Definitions, State0),
            add_result_seen(Type, import_def_edges(Id, ConnectionMapping, Row, State1)),
            add_result_updated(Type, State1)
    end.



import_def_rsc(FieldMapping, Row, State) ->


    {Callbacks, FieldMapping1} = case proplists:get_value(callbacks, FieldMapping) of
                                     undefined -> {[], FieldMapping};
                                     CB -> {CB, proplists:delete(callbacks, FieldMapping)}
                                end,
    
    %% Do the field mapping
    Props = map_fields(FieldMapping1, Row, State),

    %% Check if props mapping is valid (at least a title and a (unique) name)
    case has_required_rsc_props(Props) of
        true ->
            %% Get category name; put category ID in the record.
            Type = proplists:get_value(category, Props),
            %% Convert category name
            CatId = case name_lookup(Type, State) of
                        undefined ->
                            throw({import_error, {invalid_category_name, Type}});
                        C -> C
                    end,
            Props0 = [{category_id, CatId} | proplists:delete(category, Props)],

            %% Make row checksum
            PropK = proplists:get_keys(Props0),
            RowCS = import_checksum(Props0),

            %% Unique name of this rsc
            Name = case proplists:get_value(name, Props0) of
                       undefined ->
                           throw({import_error, {definition_without_unique_name}});
                       N -> N
                   end,

            %% Lookup existing record
            case name_lookup(Name, State) of
                %% Not exists
                undefined ->
                    Props1 = [{mf_original, Props0} | Props0],

                    %% .. insert record
                    case m_rsc_update:insert(Props1, false, State#state.context) of
                        {ok, NewId} ->
                            State1 = flush_add(NewId, State),
                            case proplists:get_value(rsc_insert, Callbacks) of
                                undefined -> none;
                                Callback -> Callback(NewId, Props1, State#state.context)
                            end,
                            {State1, {new, Type, NewId, Name}};
                        E ->
                            ?DEBUG(Type),
                            ?DEBUG(Props1),
                            ?DEBUG(Row),
                            ?DEBUG(E),
                            ?DEBUG(m_rsc:name_to_id(Name, State#state.context)),
                            ?DEBUG(name_lookup(Name, State)),
                            {State, {error, Type, E}}
                    end;

                %% Exists? 
                Id ->
                    %% Calculate resource checksum
                    RscCS = rsc_checksum(Id, PropK, State),

                    %% checksum check
                    case RowCS of
                        RscCS ->
                            %% They stayed equal; do nothing.
                            {State, {equal, Type, Id}};
                        _ ->
                            %%?DEBUG(RowCS),
                            %%?DEBUG(RscCS),
                            %% Resource has been enriched, or the import has changed.
                            {State1, Props1} = case m_rsc:p(Id, mf_original, State#state.context) of
                                         undefined ->
                                             %% No mf_original record for this rsc; update all.
                                             {State, Props0};
                                         
                                         OriginalProps ->
                                             case m_rsc:p(Id, is_protected, State#state.context) of
                                                 false ->
                                                     %% Protected flag was unchecked; update the record.
                                                     {State, [{mf_touched, {props, []}} | Props0]};
                                                 true ->
                                                     %% 3-way diff.
                                                     compare_old_new_props(Id, OriginalProps, Props0, State)
                                             end
                                     end,
                            
                            case Props1 of 
                                [] ->
                                    {State, {equal, Type, Id}};
                                Props1 ->
                                    Props2 = [{mf_original, Props0} | Props1],

                                    ?DEBUG("Updating!"),
                                    %%?DEBUG(m_rsc:p(Id, title, State)),
                                    ?DEBUG(Props2),

                                    %% .. update record and new checksum
                                    case m_rsc_update:update(Id, Props2, false, State#state.context) of
                                        {ok, Id} ->
                                            State2 = flush_add(Id, State1),
                                            case proplists:get_value(rsc_update, Callbacks) of
                                                undefined -> none;
                                                Callback -> Callback(Id, Props2, State#state.context)
                                            end,
                                            {State2, {updated, Type, Id}};
                                        E ->

                                            {State1, {error, Type, E}}
                                    end
                            end
                    end

            end;
        false ->
            {State, {ignore}}
    end.



-define(CHECKSUM(X), list_to_binary(base64:encode_to_string(crypto:sha(io_lib:format("~p", [X]))))).
%%-define(CHECKSUM(X), io_lib:format("~p", [X])).




import_checksum(Props) ->
    {_, Values} = lists:unzip(Props),
    ?CHECKSUM(Values).

rsc_checksum(Id, PropKeys, State) ->
    Values = [m_rsc:p(Id, K, State#state.context) || K <- PropKeys],
    ?CHECKSUM(Values).
    

import_def_edges({error, _}, _, _, State) ->
    State;
import_def_edges(Id, ConnectionMapping, Row, State) ->
    EdgeIds = [import_do_edge(Id, Row, Map, State) || Map <- ConnectionMapping],
    case lists:filter(fun(X) -> not(X == fail) end, EdgeIds) of
        [] ->
            State;
        NewEdgeIds ->
            managed_edge_add(Id, NewEdgeIds, State)
    end.
    


import_do_edge(Id, Row, {{PredCat, PredRowField}, {ObjectCat, ObjectRowField}}, State) ->
    % Find the predicate
    case map_one_normalize(name, PredCat, map_one(PredRowField, Row, State)) of
        <<>> ->
            %% Ignore
            fail;
        Name ->
            case name_lookup(Name, State) of
                undefined ->
                    ?DEBUG("Edge predicate does not exist?"),
                    ?DEBUG(Name),
                    fail;
                PredId ->
                    import_do_edge(Id, Row, {PredId, {ObjectCat, ObjectRowField}}, State)
            end
    end;
    
import_do_edge(Id, Row, {Predicate, {ObjectCat, ObjectRowField}}, State) ->

    % Find the object
    Name = map_one_normalize(name, ObjectCat, map_one(ObjectRowField, Row, State)),
 
    case Name of 
        <<>> ->
            %% Ignore empty
            fail;
        Name ->
            case name_lookup(Name, State) of
                %% Non exist
                undefined ->
                    %?DEBUG("Edge object/subject does not exist?"),
                    %?DEBUG(Name),
                    fail;

                %% Exists? 
                RscId ->
                    {ok, EdgeId} = m_edge:insert(Id, Predicate, RscId, State#state.context),
                    EdgeId
            end
    end;


import_do_edge(_, _, Def, _) ->
    throw({import_error, {invalid_edge_definition, Def}}).




has_required_rsc_props(Props) ->
    Required = [category, name, title],
    case lists:filter(fun(K) -> not(prop_empty(proplists:get_value(K, Props))) end, Required) of
        Required ->
            true;
        _ ->
            false
    end.


    
prop_empty(<<>>) ->
    true;
prop_empty(<<" ">>) ->
    true;
prop_empty(<<"\n">>) ->
    true;
prop_empty(undefined) ->
    true;
prop_empty(_) ->
    false.


%% Maps fields from the given mapping into a new row, filtering out empty values.
map_fields(Mapping, Row, State) ->
    Defaults = [{is_protected, true}, {is_published, true}],
    Mapped = [{K, map_one(F, Row, State)} || {K, F} <- Mapping],
    Type = case proplists:get_value(category, Mapped) of 
               undefined ->
                   throw({import_error, no_category_in_import_definition});
               T -> T
           end,        
    P = lists:filter(fun({_,Y}) -> not(prop_empty(Y)) end, [{K, map_one_normalize(K, Type, V)} || {K, V} <- Mapped]),
    add_defaults(Defaults, P).


add_defaults(D, P) ->
    add_defaults(D, P, P).

add_defaults([], _P, Acc) ->
    Acc;
add_defaults([{K,V}|Rest], P, Acc) ->
    case proplists:get_value(K, P) of 
        undefined ->
            add_defaults(Rest, P, [{K,V}|Acc]);
        _ ->
            add_defaults(Rest, P, Acc)
    end.


map_one(F, _Row, _State) when is_binary(F) -> F;
map_one(undefined, _Row, _State) -> undefined;
map_one(true, _Row, _State) -> true;
map_one(false, _Row, _State) -> false;
map_one(F, Row, _State) when is_atom(F) ->
    proplists:get_value(F, Row);
map_one(F, Row, _State) when is_function(F) ->
    F(Row);
map_one({prefer, Fields}, Row, State) ->
    map_one_prefer(Fields, Row, State);
map_one({html_escape, Value}, Row, State) ->
    z_html:escape(map_one(Value, Row, State));
map_one({concat, Fields}, Row, State) ->
    map_one_concat(Fields, Row, State);
map_one({surroundspace, Field}, Row, State) ->
    case strip(map_one(Field, Row, State)) of
        <<>> ->
            <<" ">>;
        Val ->
            list_to_binary([" ", binary_to_list(Val), " "])
    end;           
map_one({name_prefix, Prefix, Rest}, Row, State) ->
    {name_prefix, Prefix, map_one(Rest, Row, State)};

map_one({date, F}, Row, State) ->
    case map_one(F, Row, State) of
        <<"00-00-00">> ->
            {1900,1,1};
        <<"00-00-0000">> ->
            {1900,1,1};
        Val ->
            [Db, Mb, Yb] = re:split(Val, "-"),
            Year = case z_convert:to_integer(Yb) of
                       Y when Y > 1900 ->
                           Y;
                       Y when Y >= 50 andalso Y < 100 ->
                           Y + 1900;
                       Y when Y >= 0 andalso Y < 50 ->
                           Y + 2000;
                       _ ->
                           throw({error, {invalid_year, Val}})
                   end,
            Month = case z_convert:to_integer(Mb) of
                        M when M > 0 andalso M < 13 ->
                            M;
                        _ ->
                            throw({error, {invalid_month, Val}})
                    end,
            Day = case z_convert:to_integer(Db) of
                      D when D > 0 andalso D < 32 ->
                          D;
                      _ ->
                          throw({error, {invalid_day, Val}})
                  end,
            {Year, Month, Day}
    end;

map_one({time, F}, Row, State) ->
    Val = map_one(F, Row, State),
    [Hb, Mb, Sb] = re:split(Val, ":"),
    Hour = case z_convert:to_integer(Hb) of
               H when H >= 0 andalso H < 24 ->
                   H;
               H when H >= 24 andalso H < 48 ->
                   H - 24;
               _ ->
                   ?DEBUG("Invalid hour!"),
                   ?DEBUG(F),
                   0
           end,
    Minute = case z_convert:to_integer(Mb) of
               M when M >= 0 andalso M < 60 ->
                   M;
               _ ->
                   ?DEBUG("Invalid Minute!"),
                   ?DEBUG(F),
                   0
           end,
    Second = case z_convert:to_integer(Sb) of
               S when S >= 0 andalso S < 60 ->
                   S;
               _ ->
                   ?DEBUG("Invalid Second!"),
                   ?DEBUG(F),
                   0
           end,
    {Hour, Minute, Second};

map_one({datetime, D, T}, Row, State) ->
    {map_one({date, D}, Row, State),
     map_one({time, T}, Row, State)};

map_one({if_then_else, Cond, If, Else}, Row, State) ->
    case prop_empty(map_one(Cond, Row, State)) of
        false ->
            map_one(If, Row, State);
        true ->
            map_one(Else, Row, State)
    end;

map_one(F, _, _) ->
    throw({import_error, {invalid_import_definition, F}}).



map_one_prefer([F], Row, State) ->
    map_one(F, Row, State);
map_one_prefer([F|Rest], Row, State) ->
    Prop = map_one(F, Row, State),

    case prop_empty(Prop) of
        false ->
            Prop;
        true ->
            map_one_prefer(Rest, Row, State)
    end.

map_one_concat(List, Row, State) ->
    L = lists:flatten([ binary_to_list(map_one(F, Row, State)) || F <- List]),
    list_to_binary(L).


map_one_normalize(name, _Type, <<>>) ->
    <<>>;
map_one_normalize(name, _Type, {name_prefix, Prefix, V}) ->
    CheckL = 80 - size(Prefix) - 1,
    Name = case size(V) of
               L when L > CheckL ->
                   %% When name is too long, make a unique thing out of it.
                   z_string:to_name(binary_to_list(Prefix) ++ "_" ++ binary_to_list(?CHECKSUM(V)));
               _ -> 
                   z_string:to_name(binary_to_list(Prefix) ++ "_" ++ z_string:to_name(V))
           end,
    z_convert:to_binary(Name);

map_one_normalize(name, Type, V) ->
    CheckL = 80 - size(Type) - 1,
    Name = case size(V) of
               L when L > CheckL ->
                   %% When name is too long, make a unique thing out of it.
                   z_string:to_name(binary_to_list(Type) ++ "_" ++ binary_to_list(?CHECKSUM(V)));
               _ -> 
                   z_string:to_name(binary_to_list(Type) ++ "_" ++ z_string:to_name(V))
           end,
    z_convert:to_binary(Name);
map_one_normalize(_, _Type, V) ->
    V.



%% comparing props %%

compare_old_new_props(_RscId, Original, New, State) -> 
    compare_old_new_props(_RscId, Original, New, State, []).


compare_old_new_props(_RscId, _OriginalProps, [], State, Acc) -> 
    {State, Acc};

compare_old_new_props(RscId, OriginalProps, [{NewKey, NewValue} | NewProps], State, Acc) ->
    case compare_old_new_props1(RscId, NewKey, proplists:get_value(NewKey, OriginalProps),NewValue, State) of
        {_, false} ->
            compare_old_new_props(RscId, OriginalProps, NewProps, State, Acc);
        {S, true} ->
            compare_old_new_props(RscId, OriginalProps, NewProps, S, [{NewKey, NewValue} | Acc])
    end.



%% Check the original value and the new value of prop against the prop
%% in the resource.
compare_old_new_props1(Id, Prop, OrigVal0, NewVal, State) ->
    OrigTouched = case m_rsc:p(Id, mf_touched, State#state.context) of
                      undefined ->
                          [];
                      {props, T} ->
                          T;
                      [] -> []
                  end,

    %% Put surrounding paragraph tag around original value -- sometimes it is was not there yet.
    OrigVal = case Prop of
                  body ->
                      case z_convert:to_list(OrigVal0) of
                          [$<,$p,62|_] ->
                              OrigVal0;
                          [$<,$P,62|_] ->
                              OrigVal0;
                          Val ->
                              list_to_binary("<p>" ++ Val ++ "</p>")
                      end;

                  _ -> OrigVal0
              end,

    DbVal = m_rsc:p(Id, Prop, State#state.context),

    {NewTouched, Result} = 
        case {Prop, DbVal} of 

            %% special properties which are always overwritten
            {is_protected, false} ->
                {OrigTouched, true};

            {_, NewVal} ->
                %% The value in the database is equal to the to-be-updated value. 
                %% ignore this value.
                {OrigTouched, false};

            {_, OrigVal} ->
                %% The value in the database has not changed from its original value.
                %% (and is different from NewVal). Use this value.
                {lists:delete(Prop, OrigTouched), true};

            {_, DbVal} ->
                %% The value has been edited in the database.
                ?DEBUG("Edited in db!"),
                ?DEBUG(Id), ?DEBUG(Prop),
                ?DEBUG(DbVal),
                ?DEBUG(NewVal),
                ?DEBUG(OrigVal),
                {case lists:member(Prop, OrigTouched) of
                     true ->
                         OrigTouched;
                     false ->
                         [Prop | OrigTouched]
                 end,
                 false}
        end,
    case NewTouched of
        OrigTouched ->
            % ignore
            {State, Result};

        _ ->
            ?DEBUG("Value changed in db!"),
            ?DEBUG(Id),
            ?DEBUG(Prop),
            ?DEBUG(NewTouched),
            m_rsc:update(Id, [{mf_touched, {props, NewTouched}}], State#state.context),
            {flush_add(Id, State), Result}
    end.


%% Clear all items in a category.
clear_category(Name, C) ->
    Cat = m_category:name_to_id_check(Name, C),
    Ids = [Id || {Id} <- z_db:q("SELECT id FROM rsc WHERE category_id = $1", [Cat], C)],
    [m_rsc_update:delete(Id, C) || Id <- Ids],
    ok.

%% Set the "managed" flag on all items in a category
set_category_managed(Name, Record, C) ->
    Cat = m_category:name_to_id_check(Name, C),
    Ids = [Id || {Id} <- z_db:q("SELECT id FROM rsc WHERE category_id = $1", [Cat], C)],
    m_config:set_prop(import_managed_resources, Record, set, sets:from_list(Ids), C),
    ?DEBUG(length(Ids)),
    ok.



%% STATE %%

reload_name_lookup(State) ->
    D = z_db:q("SELECT name, id FROM rsc WHERE name IS NOT NULL ORDER BY name", State#state.context),
    D2 = lists:sort(D),
    State#state{name_to_id=gb_trees:from_orddict(D2)}.

add_name_lookup(State=#state{name_to_id=Tree}, Name, Id) ->
    State#state{name_to_id=gb_trees:insert(Name, Id, Tree)}.

name_lookup(Name, #state{name_to_id=Tree}) when not(is_binary(Name)) ->
    case gb_trees:lookup(list_to_binary(Name), Tree) of
        {value, V} -> V;
        none -> undefined
    end;
name_lookup(Name, #state{name_to_id=Tree}) ->
    case gb_trees:lookup(Name, Tree) of
        {value, V} -> V;
        none -> undefined
    end.


%% Adds a resource Id to the list of managed resources, if the import definition allows it.
add_managed_resource(Id, Props, State=#state{managed_resources=M}) ->
    case proplists:get_value(import_skip_delete, Props) of
        true ->
            State;
        _ ->
            State#state{managed_resources=sets:add_element(Id, M)}
    end.


reload_managed_edge_lookup(State=#state{context=C}) ->
    E = [{Id, m_rsc:p(managed_edges, {Id}, C)} || {Id} <- z_db:q("SELECT id FROM rsc", C)],
    E2=lists:sort(E),
    State#state{managed_edges=gb_trees:from_orddict(E2)}.
    
managed_edge_lookup(Id, #state{managed_edges=Edges}) ->
    managed_edge_lookup(Id, Edges);
managed_edge_lookup(Id, Edges) ->
    case gb_trees:lookup(Id, Edges) of
        {value, E} ->
            E;
        none ->
            []
    end.

managed_edge_add(Id, NewEdges, State=#state{managed_edges=Tree}) ->
    case gb_trees:lookup(Id, Tree) of
        {value, NewEdges} ->
            %% Unchanged
            State;
        {value, undefined} ->
            %% insert
            State#state{managed_edges=gb_trees:update(Id, NewEdges, Tree)};
        {value, V} ->
            ?DEBUG(V),
            State#state{managed_edges=gb_trees:update(Id, sets:to_list(sets:from_list(NewEdges ++ V)), Tree)};
        none ->
            State#state{managed_edges=gb_trees:insert(Id, NewEdges, Tree)}
    end.


flush_add(Id, State=#state{to_flush=F}) ->
    State#state{to_flush=[Id|F]}.


%% FILE IO %%

-define(CHUNK_SIZE, 1000).
-define(FS, 9).
-define(RS, 13).


read_import_file(File, Columns) ->
    %% Open the file
    ?DEBUG(File),
    Device = case file:open(File, [read, binary, unicode]) of
                 {ok, D} -> D;
                 {error, badarg} ->
                     {ok, D} = file:open(File, [read, binary]),
                     D
             end,
    %% Read all records
    Rows = scan_lines(Device),
    %% Filter out empty lines
    Rows1 = lists:filter(fun([]) -> false; (_) -> true end, Rows),
    %% Zip-in the column names
    rowszip(Rows1, Columns).

rowszip(Rows, Columns) ->
    rowszip(Rows, Columns, 1, []).
rowszip([], _Columns, _Line, Acc) ->
    lists:reverse(Acc);
rowszip([Row|Rest], Columns, Line, Acc) ->
    Zipped = try
                 lists:zip(Columns, Row)
             catch 
                 _: _->
                     throw({invalid_nr_of_columns, [{line, Line}, {columns, Columns}, {row, Row}]})
             end,
    rowszip(Rest, Columns, Line+1, [Zipped|Acc]).

strip(Binary) ->
    lstrip(rstrip(Binary)).

%% Remove whitespace chars from left side
lstrip(Binary) ->
    case Binary of
        <<X, Rest/binary>> when X < 33 ->
            lstrip(Rest);
        _ ->
            Binary
    end.
rstrip(Binary) ->
    case Binary of
        <<>> ->
            Binary;
        _ ->
            rstrip(Binary, size(Binary)-1)
    end.
rstrip(Binary, Index) ->
    case Binary of
        <<Pre:Index/binary, X>> when X < 33 ->
            rstrip(Pre, Index-1);
        _ ->
            Binary
    end.


append_field(<<>>, Field, [Row|Rows]) ->
    [[strip(Field)|Row]|Rows];
append_field(Prefix, Field, [Row|Rows]) ->
    NewField = list_to_binary(binary_to_list(Prefix) ++ binary_to_list(Field)),
    [[strip(NewField)|Row]|Rows].
append_last_field(Prefix, Field, Acc) ->
    [R|RS] = append_field(Prefix, Field, Acc),
    [lists:reverse(R)|RS].

scan_lines(Device) ->
    scan_lines(Device, <<>>, 0, [[]], <<>>).

scan_lines(Device, Chunk, Index, Acc, Remainder) ->
    case Chunk of

        <<>> ->
            %%
            %% Chunk is empty. Get the next chunk from the file.
            %%
            case io:get_chars(Device, "", ?CHUNK_SIZE) of
                eof ->
                    All = case Remainder of 
                              <<>> ->
                                  Acc;
                              _ ->
                                  append_field(<<>>, Remainder, Acc)
                          end,
                    %% Remove lastly added empty line
                    All2 = case All of
                               [[<<>>]|Rest] -> Rest;
                               _ -> All
                           end,
                    lists:reverse(All2);
                {error, E} ->
                    throw({error, E});
                NextChunk ->
                    scan_lines(Device, NextChunk, 0, Acc, Remainder)
            end;

        <<Field:Index/binary, ?FS, Rest/binary>> ->
            %%
            %% Detecting field separator. Append to current row.
            %%
            scan_lines(Device, Rest, 0, append_field(Remainder, Field, Acc), <<>>);
        
        <<Field:Index/binary, ?RS, Rest/binary>> ->
            %%
            %% Detecting record separator. Append current field; start new row.
            %%
            scan_lines(Device, Rest, 0, [ [] | append_last_field(Remainder, Field, Acc)], <<>>);
        
        <<_:Index/binary, _/binary>> ->
            %% Continue scanning
            scan_lines(Device, Chunk, Index + 1, Acc, Remainder);

        LongLine ->
            %% Long line; add to remainder.
            scan_lines(Device, <<>>, 0, Acc, list_to_binary(binary_to_list(Remainder) ++ binary_to_list(LongLine)))
    end.
